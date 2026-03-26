"""
Scraping Celery tasks.
Daily dataset collection from all sources with ML filtering.
"""
from celery import shared_task
from app.tasks.celery_app import celery_app
from datetime import datetime
import logging
from app.db.connection import mongodb
from app.analytics.metrics import MetricsCalculator

logger = logging.getLogger(__name__)


def get_all_scrapers():
    """Get dictionary of all available scrapers."""
    from app.scrapers.huggingface import HuggingFaceScraper
    from app.scrapers.kaggle import KaggleScraper
    from app.scrapers.openml import OpenMLScraper
    from app.scrapers.aws_opendata import AWSOpenDataScraper
    from app.scrapers.zenodo import ZenodoScraper
    from app.scrapers.datagov import DataGovScraper
    from app.scrapers.dataverse import HarvardDataverseScraper
    from app.scrapers.curated import CuratedDatasetsScraper
    from app.scrapers.papers_with_code import PapersWithCodeScraper
    
    return {
        # ML Platforms
        'huggingface': HuggingFaceScraper,
        'kaggle': KaggleScraper,
        'openml': OpenMLScraper,
        # Academic
        'zenodo': ZenodoScraper,
        'harvard_dataverse': HarvardDataverseScraper,
        # Government/Open Data
        'aws_opendata': AWSOpenDataScraper,
        'datagov': DataGovScraper,
        # Curated Benchmarks
        'curated': CuratedDatasetsScraper,
        'papers_with_code': PapersWithCodeScraper,
    }


# Source categories for organized scraping
SOURCE_CATEGORIES = {
    'ml_platforms': ['huggingface', 'kaggle', 'openml'],
    'academic': ['zenodo', 'harvard_dataverse'],
    'government': ['datagov', 'aws_opendata'],
    'curated': ['curated', 'papers_with_code'],
}


@shared_task(bind=True, max_retries=3)
def scrape_source(self, source_name: str, auto_save: bool = True):
    """
    Scrape datasets from a single source.
    
    Args:
        source_name: Name of the source to scrape
        auto_save: Whether to automatically save to database
    """
    try:
        logger.info(f"Starting scraping for {source_name}")
        
        scrapers = get_all_scrapers()
        
        if source_name not in scrapers:
            logger.error(f"Unknown source: {source_name}")
            return {'status': 'error', 'message': f'Unknown source: {source_name}'}
        
        # Create scraper instance
        scraper = scrapers[source_name]()
        
        # Fetch datasets
        import asyncio
        datasets = asyncio.run(scraper.scrape_with_cache())
        
        logger.info(f"Scraped {len(datasets)} datasets from {source_name}")
        
        # Auto-save to database if enabled
        saved_count = 0
        if auto_save and datasets:
            saved_count = _save_datasets_to_db(datasets)
            logger.info(f"Saved {saved_count} datasets to database from {source_name}")
        
        return {
            'status': 'success',
            'source': source_name,
            'scraped_count': len(datasets),
            'saved_count': saved_count,
            'datasets': datasets if not auto_save else None,  # Only return if not saved
        }
        
    except Exception as exc:
        logger.error(f"Scraping failed for {source_name}: {exc}")
        raise self.retry(exc=exc, countdown=60 * (2 ** self.request.retries))


def _save_datasets_to_db(datasets: list) -> int:
    """Save datasets to MongoDB with deduplication and version tracking (sync version for Celery)."""
    from app.db.connection import mongodb
    from bson import ObjectId
    from pymongo import UpdateOne
    from app.ml.quality_scorer import quality_scorer
    from app.ml.semantic_search import SemanticSearch
    
    saved_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        # Use sync client for Celery tasks
        client = mongodb.get_sync_client()
        db = client.datavault
        collection = db.datasets
        versions_collection = db.dataset_versions
        
        logger.info(f"Starting to save {len(datasets)} datasets to database (using bulk operations)")
        
        # Prepare bulk operations
        bulk_operations = []
        valid_datasets = []
        
        for dataset in datasets:
            try:
                # Add timestamp if not present
                if 'created_at' not in dataset:
                    dataset['created_at'] = datetime.utcnow()
                
                # Use source.platform + source.platform_id for deduplication
                source = dataset.get('source', {})
                platform = source.get('platform')
                platform_id = source.get('platform_id')
                canonical_name = dataset.get('canonical_name')
                
                # Validate required fields
                if not platform or not platform_id:
                    logger.warning(f"Dataset missing platform or platform_id, skipping: {canonical_name}")
                    skipped_count += 1
                    continue
                
                # Create deduplication query using platform + platform_id
                dedup_query = {
                    'source.platform': platform,
                    'source.platform_id': platform_id
                }
                
                # Compute quality score at ingest time
                try:
                    quality_breakdown = quality_scorer.get_quality_breakdown(dataset)
                    dataset['quality_score'] = quality_breakdown['overall']
                    dataset['quality_breakdown'] = quality_breakdown
                    dataset['quality_label'] = quality_scorer.get_quality_label(
                        quality_breakdown['overall']
                    )
                except Exception as qe:
                    logger.warning(f"Quality scoring failed for {canonical_name}: {qe}")
                
                # Add to bulk operations
                bulk_operations.append(
                    UpdateOne(
                        dedup_query,
                        {'$set': dataset},
                        upsert=True
                    )
                )
                valid_datasets.append((dedup_query, dataset))
                
            except Exception as e:
                logger.error(f"Error preparing dataset {dataset.get('canonical_name')}: {e}")
                error_count += 1
        
        # Execute bulk write in batches
        batch_size = 500
        total_batches = (len(bulk_operations) + batch_size - 1) // batch_size
        
        logger.info(f"Executing {len(bulk_operations)} operations in {total_batches} batches")
        
        for i in range(0, len(bulk_operations), batch_size):
            batch = bulk_operations[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                result = collection.bulk_write(batch, ordered=False)
                saved_count += result.upserted_count + result.modified_count
                logger.info(f"Batch {batch_num}/{total_batches}: {result.upserted_count} inserted, {result.modified_count} updated")
            except Exception as e:
                logger.error(f"Error in batch {batch_num}: {e}")
                error_count += len(batch)
            try:
                # Add timestamp if not present
                if 'created_at' not in dataset:
                    dataset['created_at'] = datetime.utcnow()
                
                # Use source.platform + source.platform_id for deduplication
                source = dataset.get('source', {})
                platform = source.get('platform')
                platform_id = source.get('platform_id')
                canonical_name = dataset.get('canonical_name')
                
                # Validate required fields
                if not platform or not platform_id:
                    logger.warning(f"Dataset missing platform or platform_id, skipping: {canonical_name}")
                    skipped_count += 1
                    continue
                
                # Create deduplication query using platform + platform_id
                dedup_query = {
                    'source.platform': platform,
                    'source.platform_id': platform_id
                }
                
                # Check if dataset exists and extract metrics for version tracking
                existing = collection.find_one(dedup_query)
                
                # Upsert the dataset using platform + platform_id
                result = collection.update_one(
                    dedup_query,
                    {'$set': dataset},
                    upsert=True
                )
                
                logger.debug(f"Upserted dataset: {platform}/{platform_id} - {canonical_name}")
                
                # Update Semantic Search Index
                try:
                    search_engine = SemanticSearch() # Use singleton
                    text_to_index = f"{dataset.get('canonical_name', '')} {dataset.get('description', '')} {dataset.get('domain', '')} {dataset.get('modality', '')}"
                    # We need the ID. If it's an update, we need to find the existing ID if result.upserted_id is None
                    doc_id = result.upserted_id
                    if not doc_id:
                        # Find existing
                        existing_doc = collection.find_one(dedup_query, {"_id": 1})
                        if existing_doc:
                            doc_id = existing_doc["_id"]
                    
                    if doc_id:
                        search_engine.add_to_index(str(doc_id), text_to_index)
                except Exception as se:
                    logger.error(f"Failed to add to semantic index: {se}")
                
                # Determine dataset_id for version tracking
                if result.upserted_id:
                    dataset_id = result.upserted_id
                    is_new = True
                elif existing:
                    dataset_id = existing['_id']
                    is_new = False
                else:
                    # Fallback: query for the dataset we just upserted
                    upserted_doc = collection.find_one(dedup_query)
                    dataset_id = upserted_doc['_id'] if upserted_doc else None
                    is_new = True
                
                if dataset_id:
                    # Extract current metrics
                    source_meta = dataset.get('source', {}).get('source_metadata', {})
                    size_info = dataset.get('size', {})
                    
                    new_metrics = {
                        'samples': size_info.get('samples'),
                        'downloads': source_meta.get('downloads'),
                        'likes': source_meta.get('likes'),
                        'file_size': size_info.get('file_size_bytes')
                    }
                    
                    # Check if metrics changed from existing (or if it's a new dataset)
                    should_create_version = is_new
                    
                    if existing and not is_new:
                        old_source_meta = existing.get('source', {}).get('source_metadata', {})
                        old_size_info = existing.get('size', {})
                        
                        old_metrics = {
                            'samples': old_size_info.get('samples'),
                            'downloads': old_source_meta.get('downloads'),
                            'likes': old_source_meta.get('likes'),
                            'file_size': old_size_info.get('file_size_bytes')
                        }
                        
                        # Compare metrics - if any significant field changed, create version
                        for key in ['samples', 'downloads', 'likes', 'file_size']:
                            if new_metrics.get(key) != old_metrics.get(key):
                                should_create_version = True
                                break
                    
                    if should_create_version:
                        # Mark previous versions as not current
                        versions_collection.update_many(
                            {'dataset_id': dataset_id, 'is_current': True},
                            {'$set': {'is_current': False}}
                        )
                        
                        # Count existing versions for version naming
                        version_count = versions_collection.count_documents({'dataset_id': dataset_id})
                        
                        # Create new version entry
                        version_doc = {
                            'dataset_id': dataset_id,
                            'version': f"v{version_count + 1}",
                            'timestamp': datetime.utcnow(),
                            'samples': new_metrics['samples'],
                            'downloads': new_metrics['downloads'],
                            'likes': new_metrics['likes'],
                            'file_size': new_metrics['file_size'],
                            'is_current': True
                        }
                        versions_collection.insert_one(version_doc)
                        
                        # --- Feature 6: Drift Detection ---
                        if not is_new and existing:
                            try:
                                drift_alerts = []
                                drift_score = 0
                                
                                # Compare logic (simplified version of the one in datasets.py)
                                for field, display in [
                                    ('samples', 'Sample count'),
                                    ('downloads', 'Downloads'),
                                    ('likes', 'Likes'),
                                    ('file_size', 'File size')
                                ]:
                                    curr = new_metrics.get(field)
                                    prev = old_metrics.get(field)
                                    if curr and prev and prev > 0:
                                        change = ((curr - prev) / prev) * 100
                                        if abs(change) > 20: # 20% threshold for event
                                            severity = 'high' if abs(change) > 50 else 'medium'
                                            drift_score += (30 if severity == 'high' else 15)
                                            drift_alerts.append({
                                                'field': field,
                                                'display': display,
                                                'change': round(change, 1),
                                                'severity': severity
                                            })
                                
                                if drift_alerts:
                                    event = {
                                        'dataset_id': dataset_id,
                                        'timestamp': datetime.utcnow(),
                                        'type': 'drift_detected',
                                        'score': drift_score,
                                        'alerts': drift_alerts,
                                        'version': version_doc['version']
                                    }
                                    db.drift_events.insert_one(event)
                                    logger.info(f"Drift detected for {dataset_id}: Score {drift_score}")
                                    
                                    # Trigger alert task
                                    if drift_score >= 30:
                                        from app.tasks.scraping_tasks import send_drift_alerts
                                        send_drift_alerts.delay(str(dataset_id), event)
                                        
                            except Exception as de:
                                logger.error(f"Drift detection failed for {dataset_id}: {de}")
                
                if result.upserted_id or result.modified_count:
                    saved_count += 1
                    
            except Exception as e:
                error_count += 1
                source_info = dataset.get('source', {})
                logger.error(
                    f"Failed to save dataset - Platform: {source_info.get('platform')}, "
                    f"ID: {source_info.get('platform_id')}, Name: {dataset.get('canonical_name')}, "
                    f"Error: {e}"
                )
                
        logger.info(
            f"Dataset save complete - Saved: {saved_count}, Skipped: {skipped_count}, "
            f"Errors: {error_count}, Total processed: {len(datasets)}"
        )
                
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB for saving: {e}")
    
    return saved_count


@shared_task
def run_daily_scraping():
    """
    Run daily scraping for all sources.
    Orchestrates scraping tasks for all categories.
    """
    logger.info("Starting daily scraping job for all sources")
    
    scrapers = get_all_scrapers()
    all_sources = list(scrapers.keys())
    
    # Trigger scraping for each source
    results = []
    for source in all_sources:
        result = scrape_source.delay(source, auto_save=True)
        results.append({'source': source, 'task_id': str(result.id)})
    
    logger.info(f"Triggered scraping for {len(all_sources)} sources")
    
    return {
        'status': 'started',
        'sources': all_sources,
        'tasks': results
    }


@shared_task
def scrape_category(category: str):
    """
    Scrape all sources in a category.
    
    Args:
        category: One of 'ml_platforms', 'academic', 'government', 'curated'
    """
    if category not in SOURCE_CATEGORIES:
        return {'status': 'error', 'message': f'Unknown category: {category}'}
    
    sources = SOURCE_CATEGORIES[category]
    results = []
    
    for source in sources:
        result = scrape_source.delay(source, auto_save=True)
        results.append({'source': source, 'task_id': str(result.id)})
    
    logger.info(f"Triggered scraping for category '{category}': {sources}")
    
    return {
        'status': 'started',
        'category': category,
        'sources': sources,
        'tasks': results
    }


@shared_task(bind=True, max_retries=3)
def enrich_dataset_metadata(self, dataset_id: str):
    """
    Enrich a dataset with additional metadata from Papers With Code.
    Fetches benchmarks, SOTA results, and task taxonomy.
    """
    from bson import ObjectId
    from app.scrapers.papers_with_code import PapersWithCodeScraper
    
    try:
        client = mongodb.get_sync_client()
        db = client.datavault
        
        dataset = db.datasets.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            return {"status": "error", "message": "Dataset not found"}
            
        scraper = PapersWithCodeScraper()
        
        # We need an async wrapper to run the scraper methods if they are async
        import asyncio
        loop = asyncio.get_event_loop()
        
        # PWC uses the name or platform_id as the slug
        slug = dataset.get("source", {}).get("platform_id")
        if not slug:
            # Try to match by name
            name = dataset.get("canonical_name")
            if name:
                slug = name.lower().replace(" ", "-")
        
        if not slug:
            return {"status": "skipped", "message": "No slug found for PWC mapping"}
            
        # Call enrichment (we will implement this method in PapersWithCodeScraper)
        enrichment_data = loop.run_until_complete(scraper.get_dataset_enrichment(slug))
        
        if enrichment_data:
            db.datasets.update_one(
                {"_id": ObjectId(dataset_id)},
                {"$set": {
                    "benchmarks": enrichment_data.get("benchmarks", []),
                    "intelligence.tasks": list(set(dataset.get("intelligence", {}).get("tasks", []) + enrichment_data.get("tasks", []))),
                    "intelligence.pwc_metadata": enrichment_data.get("metadata", {}),
                    "last_enriched_at": datetime.utcnow()
                }}
            )
            return {"status": "success", "enriched": True}
        
        return {"status": "success", "enriched": false}
        
    except Exception as exc:
        logger.error(f"Enrichment failed for dataset {dataset_id}: {exc}")
        raise self.retry(exc=exc, countdown=60)


@shared_task
def scrape_ml_platforms():
    """Scrape ML-focused platforms (HuggingFace, Kaggle, OpenML, etc.)."""
    return scrape_category('ml_platforms')


@shared_task
def scrape_academic_sources():
    """Scrape academic repositories (Zenodo, Harvard Dataverse)."""
    return scrape_category('academic')


@shared_task
def scrape_government_sources():
    """Scrape government/public data (Data.gov, AWS Open Data)."""
    return scrape_category('government')


@shared_task
def aggregate_metrics():
    """
    Aggregate daily metrics for all datasets.
    """
    logger.info("Starting daily metrics aggregation and trend score updates")
    
    try:
        from app.db.connection import mongodb
        # MetricsCalculator expects an async db object
        # Note: Celery tasks usually use sync client, but MetricsCalculator is built for async.
        # We'll use a local event loop to run the async methods.
        import asyncio
        
        async def run_aggregation():
            await mongodb.connect()
            calculator = MetricsCalculator(mongodb.db)
            
            # 1. Aggregate metrics for today (this would normally be yesterday but we'll run for today for testing)
            # await calculator.aggregate_daily_metrics()
            
            # 2. Update all trend scores using metadata as fallback or based on growth
            # Given we have 15k datasets, let's limit the update to the most recent ones 
            # or use the faster metadata initialization for now to refresh everything.
            await calculator.initialize_trend_scores_from_metadata()
            
            # 3. Update scores based on growth for a subset (if history exists)
            # await calculator.update_all_trend_scores(limit=1000)
            
        asyncio.run(run_aggregation())
        
    except Exception as e:
        logger.error(f"Error in metrics aggregation: {e}")
        return {'status': 'error', 'message': str(e)}
    
    return {'status': 'completed', 'timestamp': datetime.utcnow().isoformat()}


@shared_task
def get_available_sources():
    """Get list of all available scraper sources."""
    scrapers = get_all_scrapers()
    return {
        'sources': list(scrapers.keys()),
        'categories': SOURCE_CATEGORIES,
        'total_sources': len(scrapers)
    }


@shared_task
def send_drift_alerts(dataset_id: str, event: dict):
    """
    Send alerts for significant dataset drift.
    In current implementation, we log and mock webhook/email.
    """
    logger.info(f"ALERTER: High drift detected for dataset {dataset_id}!")
    logger.info(f"ALERTER: Score: {event['score']}, Alerts: {len(event['alerts'])}")
    
    # Mock webhook
    webhook_url = "https://hooks.slack.com/services/mock"
    logger.info(f"ALERTER: Sending webhook to {webhook_url}...")
    
    # Mock email for starred users (we'd fetch users who starred this dataset)
    logger.info(f"ALERTER: Notifying starred users about {event['version']} drift...")
    
    return True



"""
LLM tasks for dataset intelligence analysis.
Runs Gemini-powered analysis asynchronously via Celery.
"""
from celery import shared_task
from bson import ObjectId
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def analyze_dataset_intelligence(self, dataset_id: str):
    """
    Analyze a single dataset and extract intelligence metadata.
    
    Args:
        dataset_id: Dataset ID (string representation of ObjectId)
        
    Returns:
        dict with status and intelligence data
    """
    try:
        from app.db.connection import mongodb
        from app.llm.dataset_intelligence import dataset_intelligence_analyzer
        import asyncio
        
        # Convert string ID to ObjectId
        obj_id = ObjectId(dataset_id)
        
        # Get synchronous database client for Celery
        sync_client = mongodb.get_sync_client()
        from app.config import settings
        db = sync_client[settings.mongodb_db_name]
        
        # Fetch dataset
        dataset = db.datasets.find_one({'_id': obj_id})
        
        if not dataset:
            logger.error(f"Dataset not found: {dataset_id}")
            return {'status': 'error', 'message': 'Dataset not found'}
        
        # Fetch schema and samples
        schema = dataset.get('metadata', {}).get('schema')
        
        # Get sample data (limit to 5 samples)
        samples = list(db.dataset_samples.find(
            {'dataset_id': obj_id}
        ).limit(5))
        
        # Extract sample data
        sample_data = []
        for sample in samples:
            if 'data' in sample:
                sample_data.append(sample['data'])
        
        # Run intelligence analysis (synchronously for Celery)
        intelligence = asyncio.run(
            dataset_intelligence_analyzer.analyze_dataset(
                dataset_id=dataset_id,
                dataset_name=dataset.get('canonical_name', ''),
                description=dataset.get('description', ''),
                schema=schema,
                samples=sample_data if sample_data else None,
                metadata=dataset.get('metadata')
            )
        )
        
        if intelligence:
            # Convert to dict for storage
            intelligence_dict = intelligence.model_dump()
            
            # Convert datetime to ISO string
            if 'analyzed_at' in intelligence_dict:
                intelligence_dict['analyzed_at'] = intelligence_dict['analyzed_at'].isoformat()
            
            # Update dataset with intelligence
            db.datasets.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'intelligence': intelligence_dict,
                        'intelligence_updated_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            
            logger.info(f"Successfully analyzed dataset: {dataset.get('canonical_name')}")
            return {
                'status': 'success',
                'dataset_id': dataset_id,
                'intelligence': intelligence_dict
            }
        else:
            logger.warning(f"No intelligence generated for: {dataset.get('canonical_name')}")
            return {
                'status': 'no_intelligence',
                'dataset_id': dataset_id,
                'message': 'LLM API unavailable or failed'
            }
            
    except Exception as e:
        logger.error(f"Error analyzing dataset {dataset_id}: {e}")
        # Retry with exponential backoff
        raise self.retry(exc=e, countdown=60 * (2 ** self.request.retries))


@shared_task
def batch_analyze_datasets(dataset_ids: list, force_refresh: bool = False, rate_limit_delay: float = 3.0):
    """
    Batch analyze multiple datasets with rate limiting.
    
    Args:
        dataset_ids: List of dataset IDs (string representations)
        force_refresh: Force re-analysis even if cached
        rate_limit_delay: Seconds to wait between each task (default 3s for Gemini free tier)
        
    Returns:
        dict with batch analysis results
    """
    import time
    
    try:
        from app.db.connection import mongodb
        from app.llm.dataset_intelligence import dataset_intelligence_analyzer
        from app.config import settings
        
        # Get synchronous database client
        sync_client = mongodb.get_sync_client()
        db = sync_client[settings.mongodb_db_name]
        
        results = {
            'total': len(dataset_ids),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'rate_limit_delay': rate_limit_delay,
            'details': []
        }
        
        for i, dataset_id in enumerate(dataset_ids):
            try:
                # Check if already analyzed (unless force_refresh)
                if not force_refresh:
                    dataset = db.datasets.find_one({'_id': ObjectId(dataset_id)})
                    
                    if dataset and dataset.get('intelligence'):
                        logger.info(f"Skipping already analyzed dataset: {dataset_id}")
                        results['skipped'] += 1
                        continue
                
                # Trigger analysis task
                result = analyze_dataset_intelligence.delay(dataset_id)
                results['success'] += 1
                results['details'].append({
                    'dataset_id': dataset_id,
                    'task_id': result.id,
                    'status': 'queued'
                })
                
                # Rate limiting: wait between task submissions
                if i < len(dataset_ids) - 1 and rate_limit_delay > 0:
                    logger.info(f"Rate limiting: waiting {rate_limit_delay}s before next task...")
                    time.sleep(rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error queuing dataset {dataset_id}: {e}")
                results['failed'] += 1
                results['details'].append({
                    'dataset_id': dataset_id,
                    'error': str(e)
                })
        
        logger.info(f"Batch analysis queued: {results['success']} tasks with {rate_limit_delay}s delay")
        return results
        
    except Exception as e:
        logger.error(f"Batch analysis error: {e}")
        return {'status': 'error', 'message': str(e)}


@shared_task
def refresh_all_intelligence(limit: int = None, force_refresh: bool = False, rate_limit_delay: float = 3.0):
    """
    Refresh intelligence for all datasets with rate limiting.
    
    Args:
        limit: Optional limit on number of datasets
        force_refresh: Force re-analysis even if cached
        rate_limit_delay: Seconds to wait between tasks (default 3.0 for Gemini free tier)
        
    Returns:
        dict with refresh results
    """
    try:
        from app.db.connection import mongodb
        from app.config import settings
        
        # Get synchronous database client
        sync_client = mongodb.get_sync_client()
        db = sync_client[settings.mongodb_db_name]
        
        # Get all datasets
        query = {}
        if not force_refresh:
            # Only analyze datasets without intelligence
            query['intelligence'] = {'$exists': False}
        
        cursor = db.datasets.find(query)
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = list(cursor)
        
        dataset_ids = [str(d['_id']) for d in datasets]
        
        logger.info(f"Refreshing intelligence for {len(dataset_ids)} datasets with {rate_limit_delay}s delay")
        
        # Batch analyze with rate limiting
        return batch_analyze_datasets(dataset_ids, force_refresh=force_refresh, rate_limit_delay=rate_limit_delay)
        
    except Exception as e:
        logger.error(f"Refresh all intelligence error: {e}")
        return {'status': 'error', 'message': str(e)}


@shared_task
def batch_generate_summaries():
    """Legacy task - kept for compatibility."""
    return {'status': 'Deprecated - use analyze_dataset_intelligence instead'}

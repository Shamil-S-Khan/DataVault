"""
Admin API routes.
Endpoints for administrative tasks like triggering scrapers.
"""
from fastapi import APIRouter, HTTPException
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.delete("/datasets/clear")
async def clear_all_datasets():
    """
    Clear all datasets from the database.
    ⚠️ WARNING: This is a destructive operation!
    
    Returns:
        Number of datasets deleted
    """
    try:
        from app.db.connection import mongodb
        
        # Count before deletion
        count_before = await mongodb.db.datasets.count_documents({})
        logger.warning(f"Clearing {count_before} datasets from database")
        
        # Delete all datasets
        result = await mongodb.db.datasets.delete_many({})
        
        # Also clear dataset_sources if it exists
        sources_result = await mongodb.db.dataset_sources.delete_many({})
        
        logger.info(f"Deleted {result.deleted_count} datasets and {sources_result.deleted_count} sources")
        
        return {
            "status": "success",
            "datasets_deleted": result.deleted_count,
            "sources_deleted": sources_result.deleted_count,
            "message": "All datasets cleared successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to clear datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/scrape")
async def trigger_scraping(source: Optional[str] = None, category: Optional[str] = None):
    """
    Manually trigger dataset scraping.
    
    Args:
        source: Specific source to scrape (huggingface, kaggle, openml, zenodo, etc.)
                If None and no category, scrapes all sources
        category: Category of sources to scrape (ml_platforms, academic, government, curated)
    
    Returns:
        Status of scraping tasks
    """
    try:
        from app.tasks.scraping_tasks import scrape_source as scrape_task, run_daily_scraping, scrape_category, get_all_scrapers, SOURCE_CATEGORIES
        
        # Get valid sources dynamically
        all_scrapers = get_all_scrapers()
        valid_sources = list(all_scrapers.keys())
        valid_categories = list(SOURCE_CATEGORIES.keys())
        
        if source:
            # Scrape specific source
            if source not in valid_sources:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid source. Must be one of: {', '.join(valid_sources)}"
                )
            
            task = scrape_task.delay(source, auto_save=True)
            logger.info(f"Triggered scraping for {source}, task_id: {task.id}")
            
            return {
                "status": "started",
                "source": source,
                "task_id": str(task.id),
                "message": f"Scraping {source} in background (auto-save enabled)"
            }
            
        elif category:
            # Scrape category
            if category not in valid_categories:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid category. Must be one of: {', '.join(valid_categories)}"
                )
            
            result = scrape_category.delay(category)
            logger.info(f"Triggered scraping for category {category}, task_id: {result.id}")
            
            return {
                "status": "started",
                "category": category,
                "sources": SOURCE_CATEGORIES[category],
                "task_id": str(result.id),
                "message": f"Scraping category '{category}' in background"
            }
            
        else:
            # Scrape all sources
            result = run_daily_scraping.delay()
            logger.info(f"Triggered scraping for all sources, task_id: {result.id}")
            
            return {
                "status": "started",
                "source": "all",
                "available_sources": valid_sources,
                "task_id": str(result.id),
                "message": "Scraping all sources in background"
            }
            
    except Exception as e:
        logger.error(f"Failed to trigger scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scrape/status/{task_id}")
async def get_scraping_status(task_id: str):
    """
    Check status of a scraping task.
    
    Args:
        task_id: Celery task ID
        
    Returns:
        Task status and result
    """
    try:
        from celery.result import AsyncResult
        from app.tasks.celery_app import celery_app
        
        task = AsyncResult(task_id, app=celery_app)
        
        return {
            "task_id": task_id,
            "status": task.state,
            "result": task.result if task.ready() else None,
            "info": str(task.info) if task.info else None
        }
        
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/count")
async def get_datasets_count():
    """
    Get count of datasets in database.
    
    Returns:
        Dataset counts by collection
    """
    try:
        from app.db.connection import mongodb
        
        datasets_count = await mongodb.db.datasets.count_documents({})
        sources_count = await mongodb.db.dataset_sources.count_documents({})
        
        return {
            "datasets": datasets_count,
            "sources": sources_count
        }
        
    except Exception as e:
        logger.error(f"Failed to get counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/sample")
async def get_sample_datasets(limit: int = 10):
    """
    Get a sample of datasets from the database.
    
    Args:
        limit: Number of datasets to return
        
    Returns:
        Sample datasets
    """
    try:
        from app.db.connection import mongodb
        
        cursor = mongodb.db.datasets.find({}).limit(limit)
        datasets = await cursor.to_list(length=limit)
        
        # Format for response
        result = []
        for d in datasets:
            result.append({
                'id': str(d.get('_id')),
                'name': d.get('canonical_name'),
                'description': d.get('description', '')[:200] if d.get('description') else None,
                'domain': d.get('domain'),
                'modality': d.get('modality'),
                'source': d.get('source', {}).get('platform'),
                'created_at': str(d.get('created_at')) if d.get('created_at') else None,
            })
        
        return {"datasets": result, "count": len(result)}
        
    except Exception as e:
        logger.error(f"Failed to get sample datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/save-scraped-data")
async def save_scraped_data(task_id: str):
    """
    Save scraped data from a completed task to database.
    
    Args:
        task_id: ID of completed scraping task
        
    Returns:
        Number of datasets saved
    """
    try:
        from celery.result import AsyncResult
        from app.tasks.celery_app import celery_app
        from app.db.connection import mongodb
        from datetime import datetime
        
        # Get task result
        task = AsyncResult(task_id, app=celery_app)
        
        if not task.ready():
            raise HTTPException(status_code=400, detail="Task not completed yet")
        
        result = task.result
        
        if result.get('status') != 'success':
            raise HTTPException(status_code=400, detail="Task did not complete successfully")
        
        datasets = result.get('datasets', [])
        
        if not datasets:
            return {"status": "no_data", "datasets_saved": 0}
        
        # Save to database
        saved_count = 0
        for dataset in datasets:
            # Add timestamp if not present
            if 'created_at' not in dataset:
                dataset['created_at'] = datetime.utcnow()
            
            # Upsert based on canonical_name
            await mongodb.db.datasets.update_one(
                {'canonical_name': dataset.get('canonical_name')},
                {'$set': dataset},
                upsert=True
            )
            saved_count += 1
        
        logger.info(f"Saved {saved_count} datasets to database")
        
        return {
            "status": "success",
            "datasets_saved": saved_count,
            "source": result.get('source')
        }
        
    except Exception as e:
        logger.error(f"Failed to save scraped data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets/analyze-batch")
async def batch_analyze_datasets_admin(
    limit: Optional[int] = None,
    force_refresh: bool = False,
    rate_limit_delay: float = 3.0
):
    """
    Trigger batch intelligence analysis for datasets with rate limiting.
    Admin endpoint for analyzing multiple datasets at once.
    
    Args:
        limit: Optional limit on number of datasets to analyze
        force_refresh: Force re-analysis even if already analyzed
        rate_limit_delay: Seconds to wait between each API call (default 3.0 for Gemini free tier)
        
    Returns:
        Batch analysis status
    """
    try:
        from app.tasks.llm_tasks import refresh_all_intelligence
        
        # Trigger batch analysis with rate limiting
        task = refresh_all_intelligence.delay(
            limit=limit, 
            force_refresh=force_refresh
        )
        
        logger.info(f"Triggered batch intelligence analysis (limit: {limit}, force: {force_refresh}, delay: {rate_limit_delay}s), task_id: {task.id}")
        
        return {
            "status": "started",
            "task_id": str(task.id),
            "limit": limit,
            "force_refresh": force_refresh,
            "rate_limit_delay": rate_limit_delay,
            "message": f"Batch analysis queued for {'all' if not limit else limit} datasets with {rate_limit_delay}s delay between tasks"
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger batch analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/intelligence/stats")
async def get_intelligence_stats():
    """
    Get statistics about dataset intelligence coverage.
    
    Returns:
        Intelligence analysis statistics
    """
    try:
        from app.db.connection import mongodb
        
        total_datasets = await mongodb.db.datasets.count_documents({})
        analyzed_datasets = await mongodb.db.datasets.count_documents({
            'intelligence': {'$exists': True, '$ne': None}
        })
        
        # Get sample of analyzed datasets
        sample_cursor = mongodb.db.datasets.find({
            'intelligence': {'$exists': True, '$ne': None}
        }).limit(5)
        samples = await sample_cursor.to_list(length=5)
        
        sample_data = []
        for d in samples:
            intel = d.get('intelligence', {})
            sample_data.append({
                'id': str(d['_id']),
                'name': d.get('canonical_name'),
                'analyzed_at': d.get('intelligence_updated_at').isoformat() if d.get('intelligence_updated_at') else None,
                'tasks': intel.get('tasks', []),
                'modalities': intel.get('modalities', []),
                'domain': intel.get('domain'),
                'tags_count': len(intel.get('tags', []))
            })
        
        return {
            "total_datasets": total_datasets,
            "analyzed_datasets": analyzed_datasets,
            "unanalyzed_datasets": total_datasets - analyzed_datasets,
            "coverage_percentage": round((analyzed_datasets / total_datasets * 100), 2) if total_datasets > 0 else 0,
            "sample_analyzed": sample_data
        }
        
    except Exception as e:
        logger.error(f"Failed to get intelligence stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trend-scores/initialize")
async def initialize_trend_scores(limit: Optional[int] = None):
    """
    Initialize trend scores for all datasets based on available metadata.
    
    This calculates trend scores using:
    - Downloads count (35%)
    - Likes/stars count (25%)
    - Recency - newer datasets score higher (20%)
    - Has AI intelligence analysis (10%)
    - Has good description (10%)
    
    Args:
        limit: Optional limit on number of datasets to process
        
    Returns:
        Statistics about the update operation
    """
    try:
        from app.db.connection import mongodb
        from app.analytics.metrics import MetricsCalculator
        
        calculator = MetricsCalculator(mongodb.db)
        result = await calculator.initialize_trend_scores_from_metadata(limit=limit)
        
        return {
            "status": "success",
            **result
        }
        
    except Exception as e:
        logger.error(f"Failed to initialize trend scores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trend-scores/stats")
async def get_trend_score_stats():
    """
    Get statistics about trend scores in the database.
    
    Returns:
        Distribution of trend scores
    """
    try:
        from app.db.connection import mongodb
        
        # Get trend score distribution
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$trend_score",
                    "boundaries": [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01],
                    "default": "no_score",
                    "output": {"count": {"$sum": 1}}
                }
            }
        ]
        
        distribution = await mongodb.db.datasets.aggregate(pipeline).to_list(length=20)
        
        # Get top trending datasets
        top_trending = await mongodb.db.datasets.find(
            {"trend_score": {"$gt": 0}}
        ).sort("trend_score", -1).limit(10).to_list(length=10)
        
        return {
            "distribution": distribution,
            "top_trending": [
                {
                    "name": d.get("canonical_name"),
                    "trend_score": d.get("trend_score"),
                    "downloads": d.get("source", {}).get("source_metadata", {}).get("downloads"),
                    "platform": d.get("source", {}).get("platform")
                }
                for d in top_trending
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to get trend score stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


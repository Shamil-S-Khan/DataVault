"""
Analytics API routes.
Endpoints for topics, predictions, and trend analysis.
"""
from fastapi import APIRouter, Depends, Query
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId

from app.db.connection import get_database
from app.db.redis_client import redis_client
from app.ml.semantic_search import get_semantic_search
from app.config import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/topics")
async def get_topics(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get all discovered topics with dataset counts.
    
    Returns:
        List of topics with metadata
    """
    # Check cache
    cache_key = "analytics:topics"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Fetch topics
    topics = await db.topics.find({}).to_list(length=100)
    
    # Get dataset counts for each topic
    enriched_topics = []
    for topic in topics:
        # Count datasets with this topic
        count = await db.dataset_topics.count_documents({'topic_id': topic['_id']})
        
        enriched_topics.append({
            'id': str(topic['_id']),
            'name': topic.get('name'),
            'keywords': topic.get('keywords', []),
            'dataset_count': count
        })
    
    # Sort by dataset count
    enriched_topics.sort(key=lambda x: x['dataset_count'], reverse=True)
    
    result = {'topics': enriched_topics}
    
    # Cache for 6 hours
    await redis_client.set(cache_key, result, ttl=settings.cache_ttl_seconds)
    
    return result


@router.get("/predictions")
async def get_predictions(
    days: int = Query(30, ge=1, le=90),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get trend predictions for top datasets.
    
    Args:
        days: Number of days to look ahead
        limit: Number of datasets to return
        
    Returns:
        Predictions for top trending datasets
    """
    # Check cache
    cache_key = f"analytics:predictions:{days}:{limit}"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Get top trending datasets
    datasets = await db.datasets.find({}).sort('trend_score', -1).limit(limit).to_list(length=limit)
    
    predictions = []
    
    for dataset in datasets:
        # Get predictions for this dataset
        preds = await db.predictions.find({
            'dataset_id': dataset['_id']
        }).sort('prediction_date', 1).limit(days).to_list(length=days)
        
        if preds:
            predictions.append({
                'dataset_id': str(dataset['_id']),
                'dataset_name': dataset.get('canonical_name'),
                'current_trend_score': dataset.get('trend_score'),
                'predictions': [
                    {
                        'date': p['prediction_date'].isoformat(),
                        'predicted_score': p.get('predicted_score'),
                        'confidence_lower': p.get('confidence_lower'),
                        'confidence_upper': p.get('confidence_upper')
                    }
                    for p in preds
                ]
            })
    
    result = {'predictions': predictions}
    
    # Cache for 12 hours
    await redis_client.set(cache_key, result, ttl=12 * 3600)
    
    return result


@router.get("/recommendations/{dataset_id}")
async def get_recommendations(
    dataset_id: str,
    limit: int = Query(5, ge=1, le=20),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get recommended datasets similar to the given dataset.
    
    Args:
        dataset_id: Reference dataset ID
        limit: Number of recommendations
        
    Returns:
        List of similar datasets
    """
    if not ObjectId.is_valid(dataset_id):
        return {'error': 'Invalid dataset ID'}
    
    # Check cache
    cache_key = f"analytics:recommendations:{dataset_id}:{limit}"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached
    
    # Get semantic search instance
    search = await get_semantic_search(db)
    
    # Find similar datasets
    similar = await search.find_similar_datasets(ObjectId(dataset_id), k=limit)
    
    # Format results
    recommendations = [
        {
            'id': str(d['_id']),
            'name': d.get('canonical_name'),
            'description': d.get('description', '')[:200],
            'domain': d.get('domain'),
            'modality': d.get('modality'),
            'similarity_score': d.get('similarity_score'),
            'trend_score': d.get('trend_score')
        }
        for d in similar
    ]
    
    result = {'recommendations': recommendations}
    
    # Cache for 24 hours
    await redis_client.set(cache_key, result, ttl=24 * 3600)
    
    return result


@router.get("/trending-topics")
async def get_trending_topics(
    days: int = Query(30, ge=7, le=90),
    limit: int = Query(10, ge=1, le=20),
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get trending topics based on recent dataset growth.
    
    Args:
        days: Time window in days
        limit: Number of topics to return
        
    Returns:
        List of trending topics
    """
    # This is a simplified implementation
    # In production, you'd calculate actual growth rates per topic
    
    # Get topics with most datasets
    topics = await db.topics.find({}).to_list(length=100)
    
    topic_scores = []
    
    for topic in topics:
        # Get datasets for this topic
        dataset_topics = await db.dataset_topics.find({
            'topic_id': topic['_id']
        }).to_list(length=1000)
        
        if not dataset_topics:
            continue
        
        # Get average trend score for datasets in this topic
        dataset_ids = [dt['dataset_id'] for dt in dataset_topics]
        datasets = await db.datasets.find({
            '_id': {'$in': dataset_ids}
        }).to_list(length=1000)
        
        avg_trend_score = sum(d.get('trend_score', 0) for d in datasets) / len(datasets) if datasets else 0
        
        topic_scores.append({
            'id': str(topic['_id']),
            'name': topic.get('name'),
            'keywords': topic.get('keywords', [])[:5],
            'dataset_count': len(datasets),
            'avg_trend_score': avg_trend_score
        })
    
    # Sort by average trend score
    topic_scores.sort(key=lambda x: x['avg_trend_score'], reverse=True)
    
    return {'trending_topics': topic_scores[:limit]}


@router.get("/overview")
async def get_analytics_overview(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get system-wide analytics overview.
    
    Returns:
        Key metrics and statistics
    """
    try:
        # Total datasets
        total = await db.datasets.count_documents({})
        
        # Platform breakdown
        platform_pipeline = [
            {'$group': {
                '_id': '$source.platform',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}}
        ]
        platforms = await db.datasets.aggregate(platform_pipeline).to_list(None)
        
        # Modality breakdown
        modality_pipeline = [
            {'$group': {
                '_id': '$modality',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}}
        ]
        modalities = await db.datasets.aggregate(modality_pipeline).to_list(None)
        
        # Domain breakdown
        domain_pipeline = [
            {'$group': {
                '_id': '$domain',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]
        domains = await db.datasets.aggregate(domain_pipeline).to_list(None)
        
        # Quality score distribution
        quality_pipeline = [
            {
                '$bucket': {
                    'groupBy': '$quality_score',
                    'boundaries': [0, 0.2, 0.4, 0.6, 0.8, 1.0],
                    'default': 'unknown',
                    'output': {'count': {'$sum': 1}}
                }
            }
        ]
        quality_dist = await db.datasets.aggregate(quality_pipeline).to_list(None)
        
        # Average quality score
        avg_quality_pipeline = [
            {'$match': {'quality_score': {'$exists': True}}},
            {'$group': {
                '_id': None,
                'avg_quality': {'$avg': '$quality_score'},
                'min_quality': {'$min': '$quality_score'},
                'max_quality': {'$max': '$quality_score'}
            }}
        ]
        quality_stats = await db.datasets.aggregate(avg_quality_pipeline).to_list(None)
        
        # Size statistics
        size_pipeline = [
            {'$match': {'size.file_size_gb': {'$exists': True, '$gt': 0}}},
            {'$group': {
                '_id': None,
                'total_size_gb': {'$sum': '$size.file_size_gb'},
                'avg_size_gb': {'$avg': '$size.file_size_gb'},
                'max_size_gb': {'$max': '$size.file_size_gb'}
            }}
        ]
        size_stats = await db.datasets.aggregate(size_pipeline).to_list(None)
        
        # Trending datasets (top 10)
        trending = await db.datasets.find(
            {'trend_score': {'$exists': True}},
            {'canonical_name': 1, 'display_name': 1, 'trend_score': 1, 'domain': 1}
        ).sort('trend_score', -1).limit(10).to_list(None)
        
        return {
            'status': 'success',
            'overview': {
                'total_datasets': total,
                'platforms': {p['_id']: p['count'] for p in platforms},
                'modalities': {m['_id']: m['count'] for m in modalities},
                'top_domains': {d['_id']: d['count'] for d in domains},
                'quality_distribution': quality_dist,
                'quality_stats': quality_stats[0] if quality_stats else {},
                'size_stats': size_stats[0] if size_stats else {},
                'top_trending': [
                    {
                        'id': str(t['_id']),
                        'name': t.get('display_name') or t.get('canonical_name'),
                        'trend_score': t.get('trend_score'),
                        'domain': t.get('domain')
                    }
                    for t in trending
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting analytics overview: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail="Failed to fetch analytics")


@router.get("/quality-analytics")
async def get_quality_analytics(
    db: AsyncIOMotorDatabase = Depends(get_database)
):
    """
    Get detailed quality analytics across all datasets.
    
    Returns:
        Quality breakdown and insights
    """
    try:
        # Quality label distribution
        label_pipeline = [
            {'$match': {'quality_label': {'$exists': True}}},
            {'$group': {
                '_id': '$quality_label',
                'count': {'$sum': 1}
            }},
            {'$sort': {'count': -1}}
        ]
        label_dist = await db.datasets.aggregate(label_pipeline).to_list(None)
        
        # Quality by platform
        platform_quality_pipeline = [
            {'$match': {'quality_score': {'$exists': True}}},
            {'$group': {
                '_id': '$source.platform',
                'avg_quality': {'$avg': '$quality_score'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'avg_quality': -1}}
        ]
        platform_quality = await db.datasets.aggregate(platform_quality_pipeline).to_list(None)
        
        # Top quality datasets
        top_quality = await db.datasets.find(
            {'quality_score': {'$exists': True}},
            {'canonical_name': 1, 'display_name': 1, 'quality_score': 1, 'quality_label': 1}
        ).sort('quality_score', -1).limit(10).to_list(None)
        
        return {
            'status': 'success',
            'quality_analytics': {
                'label_distribution': {item['_id']: item['count'] for item in label_dist},
                'quality_by_platform': [
                    {
                        'platform': item['_id'],
                        'avg_quality': round(item['avg_quality'], 3),
                        'count': item['count']
                    }
                    for item in platform_quality
                ],
                'top_quality_datasets': [
                    {
                        'id': str(d['_id']),
                        'name': d.get('display_name') or d.get('canonical_name'),
                        'quality_score': d.get('quality_score'),
                        'quality_label': d.get('quality_label')
                    }
                    for d in top_quality
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting quality analytics: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail="Failed to fetch quality analytics")

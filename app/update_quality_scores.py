"""
Update quality scores for all datasets.
Run periodically to keep quality assessments up to date.
"""
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from app.ml.quality_scorer import quality_scorer
from pymongo import UpdateOne

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def update_all_quality_scores():
    """Calculate and store quality scores for all datasets."""
    # Connect to MongoDB
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    logger.info("Connected to MongoDB")
    
    # Get all datasets
    cursor = db.datasets.find({})
    datasets = await cursor.to_list(length=None)
    
    logger.info(f"Found {len(datasets)} datasets to score")
    
    # Calculate quality scores
    updates = []
    for i, dataset in enumerate(datasets, 1):
        quality_breakdown = quality_scorer.get_quality_breakdown(dataset)
        
        updates.append(
            UpdateOne(
                {'_id': dataset['_id']},
                {
                    '$set': {
                        'quality_score': quality_breakdown['overall'],
                        'quality_breakdown': quality_breakdown,
                        'quality_label': quality_scorer.get_quality_label(
                            quality_breakdown['overall']
                        )
                    }
                }
            )
        )
        
        if i % 1000 == 0:
            logger.info(f"Processed {i}/{len(datasets)} datasets")
            # Bulk write
            if updates:
                try:
                    result = await db.datasets.bulk_write(updates)
                    logger.info(f"Bulk write successful: {result.modified_count} documents modified")
                    updates = []
                except Exception as e:
                    logger.error(f"Bulk write failed: {e}")
                    updates = []
    
    # Final bulk write
    if updates:
        try:
            result = await db.datasets.bulk_write(updates)
            logger.info(f"Final bulk write successful: {result.modified_count} documents modified")
        except Exception as e:
            logger.error(f"Final bulk write failed: {e}")
    
    logger.info("Quality score update complete!")
    
    # Get statistics
    pipeline = [
        {
            '$group': {
                '_id': '$quality_label',
                'count': {'$sum': 1}
            }
        },
        {'$sort': {'count': -1}}
    ]
    
    stats = await db.datasets.aggregate(pipeline).to_list(None)
    
    logger.info("\nQuality distribution:")
    for stat in stats:
        logger.info(f"  {stat['_id']}: {stat['count']}")
    
    # Close connection
    client.close()


if __name__ == "__main__":
    asyncio.run(update_all_quality_scores())

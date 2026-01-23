"""
Migration script to populate modality field for existing datasets.
Run: docker-compose exec backend python -m app.migrate_modality
"""
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from pymongo import UpdateOne

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def infer_modality(dataset: dict) -> str:
    """Infer modality from dataset metadata."""
    # Get relevant text fields - handle None values
    name = (dataset.get('canonical_name') or '').lower()
    display_name = (dataset.get('display_name') or '').lower()
    description = (dataset.get('description') or '').lower()
    domain = (dataset.get('domain') or '').lower()
    
    # Check detected_modality from ml_filter (DataGov, Harvard, etc)
    metadata = dataset.get('metadata', {}) or {}
    if 'detected_modality' in metadata:
        return metadata['detected_modality']
    
    # Combine all text
    text = f"{name} {display_name} {description} {domain}"
    
    # Check for modality keywords
    if any(kw in text for kw in ['image', 'photo', 'picture', 'vision', 'x-ray', 'xray', 'scan', 'mnist', 'cifar', 'imagenet', 'coco', 'pixel']):
        return 'image'
    elif any(kw in text for kw in ['audio', 'sound', 'speech', 'music', 'voice', 'acoustic']):
        return 'audio'
    elif any(kw in text for kw in ['video', 'movie', 'clip', 'frame']):
        return 'video'
    elif any(kw in text for kw in ['text', 'nlp', 'language', 'corpus', 'tweet', 'comment', 'review', 'sentiment', 'document']):
        return 'text'
    elif any(kw in text for kw in ['time series', 'temporal', 'sequential', 'timeseries']):
        return 'timeseries'
    elif any(kw in text for kw in ['graph', 'network', 'node', 'edge']):
        return 'graph'
    elif any(kw in text for kw in ['geospatial', 'geographic', 'gis', 'map', 'spatial']):
        return 'geospatial'
    else:
        return 'tabular'  # Default


async def migrate_modality():
    """Migrate all existing datasets to have modality field."""
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    logger.info("Starting modality migration...")
    
    # Get all datasets without modality field
    query = {
        '$or': [
            {'modality': {'$exists': False}},
            {'modality': None},
            {'modality': ''}
        ]
    }
    
    cursor = db.datasets.find(query)
    datasets = await cursor.to_list(length=None)
    
    logger.info(f"Found {len(datasets)} datasets without modality field")
    
    if not datasets:
        logger.info("No datasets to migrate. All datasets already have modality.")
        return
    
    # Prepare bulk updates
    updates = []
    modality_counts = {}
    
    for dataset in datasets:
        modality = infer_modality(dataset)
        
        # Count modalities for stats
        modality_counts[modality] = modality_counts.get(modality, 0) + 1
        
        # Create update operation
        updates.append(
            UpdateOne(
                {'_id': dataset['_id']},
                {'$set': {'modality': modality}}
            )
        )
    
    # Execute bulk updates in batches
    batch_size = 1000
    total_updated = 0
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        result = await db.datasets.bulk_write(batch, ordered=False)
        total_updated += result.modified_count
        logger.info(f"Batch {i//batch_size + 1}/{(len(updates) + batch_size - 1)//batch_size}: Updated {result.modified_count} datasets")
    
    logger.info(f"\n{'='*60}")
    logger.info("MIGRATION COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Total datasets updated: {total_updated}")
    logger.info(f"\nModality distribution:")
    for modality, count in sorted(modality_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {modality}: {count}")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(migrate_modality())

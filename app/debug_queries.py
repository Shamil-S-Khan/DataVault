import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings

async def debug_queries():
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    # Total datasets
    total = await db.datasets.count_documents({})
    print(f'Total datasets: {total}')
    
    # Kaggle datasets
    kaggle = await db.datasets.count_documents({'source.platform': 'kaggle'})
    print(f'Kaggle datasets: {kaggle}')
    
    # Check sample kaggle dataset structure
    sample = await db.datasets.find_one({'source.platform': 'kaggle'})
    if sample:
        print(f"\nSample Kaggle dataset structure:")
        print(f"  _id: {sample.get('_id')}")
        print(f"  canonical_name: {sample.get('canonical_name')}")
        print(f"  modality: {sample.get('modality')}")
        print(f"  source.platform: {sample.get('source', {}).get('platform')}")
        print(f"  trend_score: {sample.get('trend_score')}")
    
    # Check what platforms exist
    platforms = await db.datasets.distinct('source.platform')
    print(f"\nAvailable platforms: {platforms}")
    
    # Count by platform
    print(f"\nCounts by platform:")
    for platform in platforms:
        count = await db.datasets.count_documents({'source.platform': platform})
        print(f"  {platform}: {count}")
    
    client.close()

asyncio.run(debug_queries())

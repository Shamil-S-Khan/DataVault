import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings

async def test_filter():
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    # Test platform filter with lowercase
    query = {'source.platform': 'kaggle'}
    count = await db.datasets.count_documents(query)
    print(f"Datasets with platform='kaggle': {count}")
    
    # Test with uppercase
    query2 = {'source.platform': 'Kaggle'}
    count2 = await db.datasets.count_documents(query2)
    print(f"Datasets with platform='Kaggle': {count2}")
    
    # Get sample dataset
    sample = await db.datasets.find_one({'source.platform': 'kaggle'})
    if sample:
        print(f"\nSample dataset:")
        print(f"  canonical_name: {sample.get('canonical_name')}")
        print(f"  platform: {sample.get('source', {}).get('platform')}")
        print(f"  platform_id: {sample.get('source', {}).get('platform_id')}")
        print(f"  modality: {sample.get('modality')}")
    
    client.close()

asyncio.run(test_filter())

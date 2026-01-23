import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings

async def verify():
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    # Check Kaggle image datasets
    kaggle_image = await db.datasets.count_documents({'modality': 'image', 'source.platform': 'kaggle'})
    print(f'Kaggle image datasets: {kaggle_image}')
    
    # Check OpenML image datasets
    openml_image = await db.datasets.count_documents({'modality': 'image', 'source.platform': 'openml'})
    print(f'OpenML image datasets: {openml_image}')
    
    # Check total image datasets
    total_image = await db.datasets.count_documents({'modality': 'image'})
    print(f'Total image datasets: {total_image}')
    
    # Check if Chest X-Ray exists
    chest_xray = await db.datasets.find_one({'canonical_name': {'$regex': 'chest.*xray.*pneumonia', '$options': 'i'}})
    if chest_xray:
        print(f"\nChest X-Ray dataset found:")
        print(f"  Name: {chest_xray.get('canonical_name')}")
        print(f"  Modality: {chest_xray.get('modality')}")
        print(f"  Platform: {chest_xray.get('source', {}).get('platform')}")
    else:
        print("\nChest X-Ray dataset NOT found")
    
    client.close()

asyncio.run(verify())

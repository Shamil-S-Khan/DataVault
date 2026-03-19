import asyncio
from app.db.connection import mongodb

async def main():
    await mongodb.connect()
    # Get one dataset with GQI data
    ds = await mongodb.db.datasets.find_one({'gqi_score': {'$ne': None}})
    if ds:
        print(f"ID: {ds['_id']}")
        print(f"Score: {ds['gqi_score']}")
        print(f"Grade: {ds['gqi_grade']}")
        print(f"Breakdown: {ds['gqi_breakdown']}")
    else:
        print("No GQI data found!")
    await mongodb.disconnect()

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from app.db.connection import mongodb
from app.ml.composite_scorer import composite_scorer

async def verify():
    await mongodb.connect()
    datasets = await mongodb.db.datasets.find({}, {'_id': 1, 'canonical_name': 1, 'gqi_score': 1, 'gqi_grade': 1}).limit(3).to_list(length=3)
    print("--- DB Verification ---")
    for d in datasets:
        print(f"ID: {d['_id']}, Name: {d.get('canonical_name')}, GQI: {d.get('gqi_score')}, Grade: {d.get('gqi_grade')}")
    
    # Check one specific dataset through the scorer directly to ensure it matches
    if datasets:
        test_ds = await mongodb.db.datasets.find_one({'_id': datasets[0]['_id']})
        recalc = composite_scorer.calculate_gqi(test_ds)
        print("\n--- Scorer Verification (Recalculation matches DB) ---")
        print(f"DB Score: {test_ds.get('gqi_score')}, Recalc: {recalc['score']}")
        print(f"Match: {test_ds.get('gqi_score') == recalc['score']}")

    await mongodb.disconnect()

if __name__ == "__main__":
    asyncio.run(verify())

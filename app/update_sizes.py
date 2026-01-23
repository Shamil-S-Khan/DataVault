"""Update dataset sizes in MongoDB."""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# Sizes fetched from HuggingFace API
SIZES = {
    'ylecun/mnist': 70000,
    'uoft-cs/cifar100': 60000,
    'uoft-cs/cifar10': 60000,
    'rishitdagli/cppe-5': 1029,
    'stanfordnlp/imdb': 100000,
    'rajpurkar/squad': 98169,
}

async def update():
    client = AsyncIOMotorClient("mongodb://mongodb:27017")
    db = client.datavault
    
    print("Updating dataset sizes...")
    for pid, sz in SIZES.items():
        r = await db.datasets.update_one(
            {"source.platform_id": pid}, 
            {"$set": {"size.samples": sz}}
        )
        status = "Updated" if r.modified_count else "Not found"
        print(f"  {pid}: {status} ({sz:,} samples)")
    
    print("\nAll datasets:")
    cursor = db.datasets.find({}, {"canonical_name": 1, "size.samples": 1, "source.platform_id": 1})
    async for d in cursor:
        name = d.get('canonical_name', 'Unknown')
        samples = d.get('size', {}).get('samples', 'N/A')
        if samples != 'N/A':
            samples = f"{samples:,}"
        print(f"  {name}: {samples}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(update())

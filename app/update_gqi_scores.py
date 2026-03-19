"""
Batch backfill script for GQI (Global Quality Index) scores.

Usage:
    python -m app.update_gqi_scores [--limit N]

Connects to MongoDB, iterates datasets, computes GQI, and persists
gqi_score, gqi_grade, and gqi_breakdown on each document.
"""
import asyncio
import argparse
import logging
from datetime import datetime

from app.db.connection import mongodb
from app.ml.composite_scorer import composite_scorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def backfill_gqi(limit: int | None = None):
    """Compute and persist GQI for all (or `limit`) datasets."""
    await mongodb.connect()
    db = mongodb.db

    cursor = db.datasets.find({})
    if limit:
        cursor = cursor.limit(limit)

    datasets = await cursor.to_list(length=limit or 100_000)
    total = len(datasets)
    logger.info(f"Starting GQI backfill for {total} datasets (using bulk_write)")

    batch_size = 500
    updated = 0
    errors = 0
    
    from pymongo import UpdateOne

    for i in range(0, total, batch_size):
        batch = datasets[i : i + batch_size]
        operations = []
        
        for dataset in batch:
            try:
                result = composite_scorer.calculate_gqi(dataset)
                operations.append(
                    UpdateOne(
                        {'_id': dataset['_id']},
                        {'$set': {
                            'gqi_score': result['score'],
                            'gqi_grade': result['grade'],
                            'gqi_breakdown': result['breakdown'],
                            'updated_at': datetime.utcnow(),
                        }}
                    )
                )
            except Exception as e:
                errors += 1
                logger.error(f"Error computing GQI for {dataset.get('canonical_name', dataset['_id'])}: {e}")

        if operations:
            try:
                await db.datasets.bulk_write(operations, ordered=False)
                updated += len(operations)
                logger.info(f"Progress: {min(i + batch_size, total)}/{total} ({updated} updated, {errors} errors)")
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                errors += len(operations)

    logger.info(f"Backfill complete: {updated} updated, {errors} errors out of {total}")
    await mongodb.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Backfill GQI scores")
    parser.add_argument("--limit", type=int, default=None, help="Max datasets to process")
    args = parser.parse_args()
    asyncio.run(backfill_gqi(args.limit))


if __name__ == "__main__":
    main()

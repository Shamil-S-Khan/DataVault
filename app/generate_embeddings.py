"""
Generate embeddings for all datasets in the database.
Run this once to initialize the recommendation system.
"""
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
from app.ml.recommender import DatasetRecommender

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def generate_all_embeddings():
    """Generate embeddings for all datasets."""
    # Connect to MongoDB
    client = AsyncIOMotorClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    
    logger.info("Connected to MongoDB")
    
    # Initialize recommender
    recommender = DatasetRecommender(db)
    
    # Generate embeddings in batches
    logger.info("Starting embedding generation...")
    await recommender.generate_embeddings_batch(batch_size=100)
    
    logger.info("Embedding generation complete!")
    
    # Close connection
    client.close()


if __name__ == "__main__":
    asyncio.run(generate_all_embeddings())

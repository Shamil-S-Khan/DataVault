"""
MongoDB connection management with connection pooling.
Optimized for MongoDB Atlas free tier (512MB, 100 concurrent connections).
"""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import MongoClient
from typing import Optional
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class MongoDB:
    """MongoDB connection manager."""
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self.sync_client: Optional[MongoClient] = None
        
    async def connect(self):
        """Establish connection to MongoDB."""
        try:
            import certifi
            self.client = AsyncIOMotorClient(
                settings.mongodb_uri,
                maxPoolSize=settings.max_mongodb_connections,
                minPoolSize=2,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=20000,
                tlsCAFile=certifi.where()
            )
            self.db = self.client[settings.mongodb_db_name]
            
            # Test connection (with timeout to avoid hanging startup)
            try:
                import asyncio
                await asyncio.wait_for(self.client.admin.command('ping'), timeout=5.0)
                logger.info(f"Connected to MongoDB: {settings.mongodb_db_name}")
            except Exception as ping_e:
                logger.warning(f"MongoDB ping failed, but proceeding: {ping_e}")

            
            # Create indexes
            await self._create_indexes()
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    def get_sync_client(self) -> MongoClient:
        """Get synchronous MongoDB client for Celery tasks."""
        if not self.sync_client:
            import certifi
            self.sync_client = MongoClient(
                settings.mongodb_uri,
                maxPoolSize=settings.max_mongodb_connections,
                minPoolSize=2,
                tlsCAFile=certifi.where()
            )
        return self.sync_client
    
    async def _create_indexes(self):
        """Create database indexes for optimal query performance."""
        logger.info("Creating MongoDB indexes...")
        
        try:
            # Datasets collection indexes
            await self.db.datasets.create_index("canonical_name", unique=True)
            await self.db.datasets.create_index("domain")
            await self.db.datasets.create_index("modality")
            await self.db.datasets.create_index("trend_score")
            await self.db.datasets.create_index("quality_score")
            await self.db.datasets.create_index("created_at")
            await self.db.datasets.create_index("updated_at")
            # Text index for full-text search
            await self.db.datasets.create_index([
                ("canonical_name", "text"),
                ("description", "text"),
                ("llm_summary", "text")
            ])
            
            # Dataset sources indexes
            await self.db.dataset_sources.create_index("dataset_id")
            await self.db.dataset_sources.create_index([("platform", 1), ("platform_id", 1)], unique=True)
            
            # Metrics daily indexes
            await self.db.metrics_daily.create_index([("dataset_id", 1), ("date", -1)])
            await self.db.metrics_daily.create_index("date")
            # TTL index to auto-delete old metrics (90 days)
            if settings.enable_archival:
                try:
                    # Drop existing date index if it exists without TTL
                    await self.db.metrics_daily.drop_index("date_1")
                except:
                    pass  # Index doesn't exist, that's fine
                
                await self.db.metrics_daily.create_index(
                    "date",
                    expireAfterSeconds=settings.archival_days * 24 * 60 * 60
                )
            
            # Topics indexes
            await self.db.topics.create_index("name", unique=True)
            
            # Dataset topics indexes
            await self.db.dataset_topics.create_index("dataset_id")
            await self.db.dataset_topics.create_index("topic_id")
            await self.db.dataset_topics.create_index([("dataset_id", 1), ("topic_id", 1)], unique=True)
            
            # Predictions indexes
            await self.db.predictions.create_index([("dataset_id", 1), ("prediction_date", -1)])
            await self.db.predictions.create_index("created_at")
            
            # Users indexes
            await self.db.users.create_index("email", unique=True)
            
            # Reviews indexes
            await self.db.reviews.create_index("dataset_id")
            await self.db.reviews.create_index("user_id")
            await self.db.reviews.create_index([("user_id", 1), ("dataset_id", 1)], unique=True)
            
            # User activity indexes
            await self.db.user_activity.create_index([("user_id", 1), ("dataset_id", 1)], unique=True)
            await self.db.user_activity.create_index("last_viewed")
            
            # Anomalies indexes
            await self.db.anomalies.create_index("dataset_id")
            await self.db.anomalies.create_index("detected_at")
            
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Some indexes may already exist or have conflicts: {e}")
            # Don't fail startup due to index issues


# Global MongoDB instance
mongodb = MongoDB()


async def get_database() -> AsyncIOMotorDatabase:
    """Dependency to get database instance."""
    return mongodb.db

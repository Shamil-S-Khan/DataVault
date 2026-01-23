"""
Redis client for caching and rate limiting.
Optimized for Upstash free tier (10,000 commands/day).
"""
import redis.asyncio as aioredis
import redis
from typing import Optional, Any
import json
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis connection manager with caching utilities."""
    
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None
        self.sync_client: Optional[redis.Redis] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def connect(self):
        """Establish connection to Redis."""
        try:
            import asyncio
            current_loop = asyncio.get_running_loop()
            
            # If client exists, check if its loop is still running and matches current
            if self.client:
                # Check if loop changed or is no longer running
                if self._loop != current_loop or not current_loop.is_running():
                    self.client = None
                else:
                    try:
                        await self.client.ping()
                    except Exception:
                        self.client = None
            
            if self.client is None:
                self.client = await aioredis.from_url(
                    settings.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=settings.max_redis_connections,
                )
                self._loop = current_loop
                logger.debug("Connected to Redis (new loop)")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self):
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis")
    
    def get_sync_client(self) -> redis.Redis:
        """Get synchronous Redis client for Celery."""
        if not self.sync_client:
            self.sync_client = redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self.sync_client
    
    async def _ensure_connected(self):
        """Ensure Redis client is connected."""
        if self.client is None:
            await self.connect()
            
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            await self._ensure_connected()
            value = await self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Redis GET error for key {key}: {e}")
            return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """Set value in cache with optional TTL."""
        try:
            await self._ensure_connected()
            serialized = json.dumps(value)
            if ttl:
                await self.client.setex(key, ttl, serialized)
            else:
                await self.client.set(key, serialized)
            return True
        except Exception as e:
            logger.error(f"Redis SET error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        try:
            await self._ensure_connected()
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis DELETE error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        try:
            await self._ensure_connected()
            return await self.client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis EXISTS error for key {key}: {e}")
            return False
    
    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment counter."""
        try:
            await self._ensure_connected()
            return await self.client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Redis INCR error for key {key}: {e}")
            return None
    
    async def expire(self, key: str, ttl: int) -> bool:
        """Set expiration on key."""
        try:
            await self._ensure_connected()
            await self.client.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Redis EXPIRE error for key {key}: {e}")
            return False
    
    async def get_many(self, keys: list) -> dict:
        """Get multiple keys at once."""
        try:
            await self._ensure_connected()
            values = await self.client.mget(keys)
            result = {}
            for key, value in zip(keys, values):
                if value:
                    result[key] = json.loads(value)
            return result
        except Exception as e:
            logger.error(f"Redis MGET error: {e}")
            return {}
    
    async def set_many(self, mapping: dict, ttl: Optional[int] = None) -> bool:
        """Set multiple key-value pairs."""
        try:
            await self._ensure_connected()
            serialized = {k: json.dumps(v) for k, v in mapping.items()}
            await self.client.mset(serialized)
            
            if ttl:
                # Set expiration for all keys
                pipeline = self.client.pipeline()
                for key in mapping.keys():
                    pipeline.expire(key, ttl)
                await pipeline.execute()
            
            return True
        except Exception as e:
            logger.error(f"Redis MSET error: {e}")
            return False


# Global Redis instance
redis_client = RedisClient()


async def get_redis() -> aioredis.Redis:
    """Dependency to get Redis client."""
    return redis_client.client

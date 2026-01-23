"""
Base scraper class with rate limiting, retry logic, and caching.
All source-specific scrapers inherit from this class.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import logging
import time
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
import requests
from app.config import settings
from app.db.redis_client import redis_client

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for all dataset scrapers.
    Implements common functionality like rate limiting, caching, and error handling.
    """
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataVault/1.0 (Dataset Discovery Platform)'
        })
        
    @abstractmethod
    async def fetch_datasets(self) -> List[Dict[str, Any]]:
        """
        Fetch datasets from the source.
        Must be implemented by each scraper.
        
        Returns:
            List of dataset dictionaries with standardized fields
        """
        pass
    
    @abstractmethod
    def normalize_dataset(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw dataset data to standard format.
        
        Args:
            raw_data: Raw dataset data from source
            
        Returns:
            Normalized dataset dictionary
        """
        pass
    
    @sleep_and_retry
    @limits(calls=settings.rate_limit_requests_per_minute, period=60)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    def make_request(
        self,
        url: str,
        method: str = "GET",
        **kwargs
    ) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            url: URL to request
            method: HTTP method (GET, POST, etc.)
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails after retries
        """
        try:
            logger.debug(f"Making {method} request to {url}")
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            
            # Add delay to respect rate limits
            time.sleep(settings.scraper_delay_seconds)
            
            return response
            
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    async def get_cached_data(self, cache_key: str) -> Optional[Any]:
        """
        Get data from Redis cache.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached data or None
        """
        try:
            return await redis_client.get(cache_key)
        except Exception as e:
            logger.warning(f"Cache retrieval failed for {cache_key}: {e}")
            return None
    
    async def set_cached_data(
        self,
        cache_key: str,
        data: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Store data in Redis cache.
        
        Args:
            cache_key: Cache key
            data: Data to cache (will be serialized with datetime conversion)
            ttl: Time to live in seconds (default: 24 hours)
            
        Returns:
            True if successful
        """
        try:
            # Convert datetime objects to ISO strings before caching
            data = self._serialize_for_cache(data)
            ttl = ttl or settings.cache_ttl_seconds
            return await redis_client.set(cache_key, data, ttl)
        except Exception as e:
            logger.warning(f"Cache storage failed for {cache_key}: {e}")
            return False
    
    def _serialize_for_cache(self, obj: Any) -> Any:
        """
        Recursively convert datetime objects to ISO strings for JSON serialization.
        
        Args:
            obj: Object to serialize (dict, list, or primitive)
            
        Returns:
            Serializable version of the object
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._serialize_for_cache(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_cache(item) for item in obj]
        else:
            return obj
    
    def extract_metadata(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract common metadata fields.
        
        Args:
            raw_data: Raw dataset data
            
        Returns:
            Metadata dictionary
        """
        metadata = {}
        
        # Common fields to extract
        fields = [
            'downloads', 'stars', 'forks', 'watchers', 'citations',
            'views', 'likes', 'votes', 'usability_score', 'paper_count',
            'contributors', 'last_updated', 'version'
        ]
        
        for field in fields:
            if field in raw_data:
                metadata[field] = raw_data[field]
        
        return metadata
    
    def create_standard_dataset(
        self,
        name: str,
        description: str,
        url: str,
        platform_id: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create standardized dataset dictionary.
        
        Args:
            name: Dataset name
            description: Dataset description
            url: Dataset URL
            platform_id: ID on source platform
            **kwargs: Additional fields
            
        Returns:
            Standardized dataset dictionary
        """
        return {
            'canonical_name': name,
            'description': description or '',
            'source': {
                'platform': self.source_name,
                'platform_id': platform_id,
                'url': url,
                'source_metadata': kwargs.get('metadata', {})
            },
            'domain': kwargs.get('domain'),
            'modality': kwargs.get('modality'),
            'size': {
                'samples': kwargs.get('samples'),
                'file_size_gb': kwargs.get('file_size_gb')
            },
            'license': kwargs.get('license'),
            'created_at': kwargs.get('created_at', datetime.utcnow()),
            'trend_score': self._calculate_initial_trend_score(kwargs),
            'metadata': kwargs.get('metadata', {})
        }
    
    def _calculate_initial_trend_score(self, kwargs: Dict[str, Any]) -> float:
        """
        Calculate initial trend score based on available metadata.
        
        Args:
            kwargs: Dataset metadata including source_metadata
            
        Returns:
            Initial trend score (0-1)
        """
        # Prioritize provided trend_score if it exists
        if 'trend_score' in kwargs:
            return float(kwargs['trend_score'])
            
        metadata = kwargs.get('metadata', {})
        
        # Extract key metrics
        downloads = metadata.get('downloads', 0)
        likes = metadata.get('likes', 0)
        votes = metadata.get('votes', 0)
        views = metadata.get('views', 0)
        usability_score = metadata.get('usability_score', 0)
        
        # Normalize metrics to 0-1 using log scale
        import math
        
        def log_normalize(value, max_val=1000000):
            if value <= 0:
                return 0
            return min(1.0, math.log10(value + 1) / math.log10(max_val))
        
        # Calculate component scores
        downloads_score = log_normalize(downloads, 100000)  # 100k downloads = max
        popularity_score = log_normalize(likes + votes, 10000)  # 10k likes/votes = max
        engagement_score = log_normalize(views, 50000)  # 50k views = max
        quality_score = usability_score / 10.0 if usability_score > 0 else 0.5  # Kaggle usability 0-10
        
        # Recency bonus - newer datasets get a boost
        created_at = kwargs.get('created_at')
        if isinstance(created_at, datetime):
            days_old = (datetime.utcnow() - created_at).days
        else:
            days_old = 180  # Default to 6 months
        
        recency_score = max(0.1, 1.0 - (days_old / 365))  # Decay over 1 year, min 0.1
        
        # Weighted combination
        trend_score = (
            downloads_score * 0.3 +
            popularity_score * 0.25 +
            engagement_score * 0.2 +
            quality_score * 0.15 +
            recency_score * 0.1
        )
        
        # Ensure score is between 0.01 and 1.0 (avoid 0 so datasets appear)
        trend_score = max(0.01, min(1.0, trend_score))
        
        return trend_score
    
    async def scrape_with_cache(self) -> List[Dict[str, Any]]:
        """
        Scrape datasets with caching support.
        
        Returns:
            List of normalized datasets
        """
        cache_key = f"scraper:{self.source_name}:datasets"
        
        # Try to get from cache first
        cached_data = await self.get_cached_data(cache_key)
        if cached_data:
            logger.info(f"Using cached data for {self.source_name}")
            return cached_data
        
        # Fetch fresh data
        logger.info(f"Fetching fresh data from {self.source_name}")
        try:
            datasets = await self.fetch_datasets()
            
            # Cache the results
            await self.set_cached_data(cache_key, datasets)
            
            logger.info(f"Scraped {len(datasets)} datasets from {self.source_name}")
            return datasets
            
        except Exception as e:
            logger.error(f"Scraping failed for {self.source_name}: {e}")
            return []
    
    def __del__(self):
        """Close session on cleanup."""
        if hasattr(self, 'session'):
            self.session.close()

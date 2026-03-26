"""
Centralized configuration management using Pydantic Settings.
Loads environment variables and provides type-safe configuration access.
"""
from pydantic_settings import BaseSettings
from pydantic import field_validator, model_validator
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # MongoDB Configuration
    mongodb_uri: str
    mongodb_db_name: str = "datavault"
    max_mongodb_connections: int = 20
    
    # Redis Configuration
    redis_url: Optional[str] = None
    max_redis_connections: int = 10
    upstash_redis_rest_url: Optional[str] = None
    upstash_redis_rest_token: Optional[str] = None
    
    # API Keys - Data Sources
    kaggle_username: Optional[str] = None
    kaggle_key: Optional[str] = None
    github_token: Optional[str] = None
    
    # LLM API
    gemini_api_key: Optional[str] = None
    grok_api_key: Optional[str] = None
    groq_api_key: Optional[str] = None
    huggingface_api_key: Optional[str] = None  # HuggingFace Inference API
    llm_provider: str = "huggingface"  # "gemini", "huggingface", "grok", or "groq"
    
    # Application Settings
    environment: str = "development"
    debug: bool = True
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: List[str] | str = ["http://localhost:3000"]
    
    # Celery Configuration
    celery_broker_url: str
    celery_result_backend: str
    
    # Rate Limiting
    rate_limit_requests_per_minute: int = 60
    scraper_delay_seconds: float = 1.0
    
    # Caching
    cache_ttl_seconds: int = 21600  # 6 hours
    search_cache_ttl_seconds: int = 3600  # 1 hour
    
    # Free Tier Optimization
    enable_compression: bool = True
    enable_archival: bool = True
    archival_days: int = 90
    
    # Sentry
    sentry_dsn: Optional[str] = None
    
    # Feature Gating
    disable_tier_gating: bool = True
    
    # NextAuth
    nextauth_secret: str = "dev-secret"
    nextauth_url: str = "http://localhost:3000"
    
    # OAuth Providers
    github_client_id: Optional[str] = None
    github_client_secret: Optional[str] = None
    google_client_id: Optional[str] = None
    google_client_secret: Optional[str] = None
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    @field_validator("debug", mode="before")
    @classmethod
    def _parse_debug(cls, value):
        """Accept broader env values (e.g., 'release') for DEBUG."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "debug", "development", "dev"}:
                return True
            if normalized in {"0", "false", "no", "off", "release", "prod", "production"}:
                return False
        return bool(value)

    @model_validator(mode="after")
    def _ensure_redis_url(self):
        """Backwards-compatible Redis resolution for local/dev envs."""
        if not self.redis_url:
            self.redis_url = "redis://localhost:6379"
        
        # Fallback for Groq key if user set it as GROK_API_KEY
        if not self.groq_api_key and self.grok_api_key:
            self.groq_api_key = self.grok_api_key
            
        return self
        
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS origins from comma-separated string if needed."""
        if isinstance(self.cors_origins, str):
            return [origin.strip() for origin in self.cors_origins.split(",")]
        return self.cors_origins


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Uses lru_cache to ensure settings are loaded only once.
    """
    return Settings()


# Global settings instance
settings = get_settings()

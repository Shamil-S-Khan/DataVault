"""
Pydantic models for MongoDB documents.
Provides type safety and validation for database operations.
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from bson import ObjectId


class PyObjectId(ObjectId):
    """Custom ObjectId type for Pydantic."""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)
    
    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")


class DatasetSize(BaseModel):
    """Dataset size information."""
    samples: Optional[int] = None
    file_size_gb: Optional[float] = None


class Dataset(BaseModel):
    """Main dataset document model."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    canonical_name: str
    description: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    domain: Optional[str] = None
    modality: Optional[str] = None
    size: Optional[DatasetSize] = None
    license: Optional[str] = None
    quality_score: Optional[float] = None
    trend_score: Optional[float] = None
    llm_summary: Optional[str] = None
    llm_insights: Optional[List[str]] = None
    
    # Intelligence metadata (AI-extracted)
    intelligence: Optional[Dict[str, Any]] = None
    intelligence_updated_at: Optional[datetime] = None
    intelligence_version: str = "1.0"
    
    # GQI (Global Quality Index) fields
    gqi_score: Optional[float] = None
    gqi_grade: Optional[str] = None
    gqi_breakdown: Optional[Dict[str, Any]] = None
    
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding_vector: Optional[List[float]] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class DatasetSource(BaseModel):
    """Dataset source tracking (which platform it came from)."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    platform: str  # e.g., "kaggle", "huggingface", "github"
    platform_id: str  # ID on the source platform
    url: str
    source_metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class MetricsDaily(BaseModel):
    """Daily aggregated metrics for a dataset."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    date: datetime
    downloads: Optional[int] = None
    stars: Optional[int] = None
    citations: Optional[int] = None
    views: Optional[int] = None
    growth_rate_7d: Optional[float] = None
    growth_rate_30d: Optional[float] = None
    growth_rate_90d: Optional[float] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class Topic(BaseModel):
    """Topic from topic modeling."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    name: str
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    coherence_score: Optional[float] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class DatasetTopic(BaseModel):
    """Association between datasets and topics."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    topic_id: PyObjectId
    score: float  # Probability/relevance score
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class Prediction(BaseModel):
    """Trend prediction for a dataset."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    prediction_date: datetime
    predicted_score: float
    confidence_lower: Optional[float] = None
    confidence_upper: Optional[float] = None
    model_type: str  # e.g., "prophet", "arima", "random_forest"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class User(BaseModel):
    """User account model."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    email: str
    name: Optional[str] = None
    image: Optional[str] = None
    preferences: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class Review(BaseModel):
    """User review for a dataset."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    user_id: PyObjectId
    dataset_id: PyObjectId
    rating: int  # 1-5 stars
    comment: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class UserActivity(BaseModel):
    """Track user interactions with datasets."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    user_id: PyObjectId
    dataset_id: PyObjectId
    view_count: int = 0
    last_viewed: datetime = Field(default_factory=datetime.utcnow)
    bookmarked: bool = False
    bookmarked_at: Optional[datetime] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class Anomaly(BaseModel):
    """Detected anomaly in dataset metrics."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    anomaly_type: str  # e.g., "isolation_forest", "lof", "zscore"
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    metric_name: str
    metric_value: float
    anomaly_score: float
    description: Optional[str] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )


class DatasetVersion(BaseModel):
    """Version snapshot for tracking dataset changes over time."""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    dataset_id: PyObjectId
    version: str  # e.g., "v1", "v2" or date-based like "2026-01-16"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    samples: Optional[int] = None
    downloads: Optional[int] = None
    likes: Optional[int] = None
    file_size: Optional[int] = None  # in bytes
    is_current: bool = False
    
    # Optional: store what changed
    changes: Optional[Dict[str, Any]] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )

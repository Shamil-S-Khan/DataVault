"""
Popularity metrics and trend calculation.
Computes rolling windows, growth rates, and trend scores.
"""
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """Calculate popularity metrics and trend scores for datasets."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
    
    async def calculate_growth_rates(
        self,
        dataset_id: ObjectId,
        windows: List[int] = [7, 30, 90]
    ) -> Dict[str, float]:
        """
        Calculate growth rates for different time windows.
        
        Args:
            dataset_id: Dataset ID
            windows: List of window sizes in days
            
        Returns:
            Dictionary of growth rates
        """
        growth_rates = {}
        
        for window in windows:
            start_date = datetime.utcnow() - timedelta(days=window)
            
            # Fetch metrics for the window
            cursor = self.db.metrics_daily.find({
                'dataset_id': dataset_id,
                'date': {'$gte': start_date}
            }).sort('date', 1)
            
            metrics = await cursor.to_list(length=window)
            
            if len(metrics) < 2:
                growth_rates[f'growth_rate_{window}d'] = 0.0
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(metrics)
            
            # Calculate growth rate for each metric
            metric_fields = ['downloads', 'stars', 'citations', 'views']
            rates = []
            
            for field in metric_fields:
                if field in df.columns and df[field].notna().sum() > 1:
                    first_val = df[field].iloc[0]
                    last_val = df[field].iloc[-1]
                    
                    if first_val > 0:
                        rate = (last_val - first_val) / first_val
                        rates.append(rate)
            
            # Average growth rate across metrics
            growth_rates[f'growth_rate_{window}d'] = np.mean(rates) if rates else 0.0
        
        return growth_rates
    
    async def calculate_velocity(
        self,
        dataset_id: ObjectId,
        window: int = 7
    ) -> float:
        """
        Calculate velocity (acceleration of growth).
        
        Args:
            dataset_id: Dataset ID
            window: Window size in days
            
        Returns:
            Velocity metric
        """
        start_date = datetime.utcnow() - timedelta(days=window * 2)
        
        cursor = self.db.metrics_daily.find({
            'dataset_id': dataset_id,
            'date': {'$gte': start_date}
        }).sort('date', 1)
        
        metrics = await cursor.to_list(length=window * 2)
        
        if len(metrics) < window:
            return 0.0
        
        df = pd.DataFrame(metrics)
        
        # Calculate velocity as change in growth rate
        metric_fields = ['downloads', 'stars', 'citations']
        velocities = []
        
        for field in metric_fields:
            if field in df.columns and df[field].notna().sum() > window:
                # Calculate rolling growth rate
                df[f'{field}_growth'] = df[field].pct_change()
                
                # Velocity is change in growth rate
                recent_growth = df[f'{field}_growth'].tail(window).mean()
                older_growth = df[f'{field}_growth'].head(window).mean()
                
                if not np.isnan(recent_growth) and not np.isnan(older_growth):
                    velocity = recent_growth - older_growth
                    velocities.append(velocity)
        
        return np.mean(velocities) if velocities else 0.0
    
    async def calculate_trend_score(
        self,
        dataset_id: ObjectId,
        weights: Optional[Dict[str, float]] = None
    ) -> float:
        """
        Calculate weighted trend score combining multiple signals.
        
        Args:
            dataset_id: Dataset ID
            weights: Custom weights for different signals
            
        Returns:
            Trend score (0-1)
        """
        if weights is None:
            weights = {
                'growth_7d': 0.3,
                'growth_30d': 0.25,
                'growth_90d': 0.15,
                'velocity': 0.2,
                'recency': 0.1
            }
        
        # Get growth rates
        growth_rates = await self.calculate_growth_rates(dataset_id)
        
        # Get velocity
        velocity = await self.calculate_velocity(dataset_id)
        
        # Get dataset for recency
        dataset = await self.db.datasets.find_one({'_id': dataset_id})
        
        # Calculate recency score (newer datasets get higher scores)
        if dataset and dataset.get('created_at'):
            days_old = (datetime.utcnow() - dataset['created_at']).days
            recency_score = max(0, 1 - (days_old / 365))  # Decay over 1 year
        else:
            recency_score = 0.5
        
        # Normalize growth rates to 0-1 range using sigmoid
        def sigmoid(x):
            return 1 / (1 + np.exp(-x))
        
        signals = {
            'growth_7d': sigmoid(growth_rates.get('growth_rate_7d', 0) * 10),
            'growth_30d': sigmoid(growth_rates.get('growth_rate_30d', 0) * 10),
            'growth_90d': sigmoid(growth_rates.get('growth_rate_90d', 0) * 10),
            'velocity': sigmoid(velocity * 100),
            'recency': recency_score
        }
        
        # Calculate weighted score
        trend_score = sum(signals[k] * weights[k] for k in weights.keys())
        
        # Ensure score is between 0 and 1
        trend_score = max(0.0, min(1.0, trend_score))
        
        return trend_score
    
    async def update_all_trend_scores(self, limit: Optional[int] = None):
        """
        Update trend scores for all datasets.
        
        Args:
            limit: Optional limit on number of datasets to update
        """
        logger.info("Starting trend score update for all datasets")
        
        # Get all datasets
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 10000)
        
        updated_count = 0
        
        for dataset in datasets:
            try:
                # Calculate trend score
                trend_score = await self.calculate_trend_score(dataset['_id'])
                
                # Update dataset
                await self.db.datasets.update_one(
                    {'_id': dataset['_id']},
                    {
                        '$set': {
                            'trend_score': trend_score,
                            'updated_at': datetime.utcnow()
                        }
                    }
                )
                
                updated_count += 1
                
                if updated_count % 100 == 0:
                    logger.info(f"Updated {updated_count} datasets")
                
            except Exception as e:
                logger.error(f"Error updating trend score for {dataset['_id']}: {e}")
        
        logger.info(f"Trend score update complete: {updated_count} datasets updated")
        
        return updated_count
    
    async def aggregate_daily_metrics(self, date: Optional[datetime] = None):
        """
        Aggregate metrics for a specific date.
        
        Args:
            date: Date to aggregate (defaults to yesterday)
        """
        if date is None:
            date = datetime.utcnow() - timedelta(days=1)
        
        # Set to start of day
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"Aggregating metrics for {date.date()}")
        
        # Get all datasets
        datasets = await self.db.datasets.find({}).to_list(length=10000)
        
        for dataset in datasets:
            try:
                # Get metrics from sources (this would be populated by scrapers)
                # For now, we'll create placeholder metrics
                
                # Calculate growth rates
                growth_rates = await self.calculate_growth_rates(dataset['_id'])
                
                # Create or update metrics document
                await self.db.metrics_daily.update_one(
                    {
                        'dataset_id': dataset['_id'],
                        'date': date
                    },
                    {
                        '$set': {
                            **growth_rates,
                            'updated_at': datetime.utcnow()
                        }
                    },
                    upsert=True
                )
                
            except Exception as e:
                logger.error(f"Error aggregating metrics for {dataset['_id']}: {e}")
        
        logger.info("Daily metrics aggregation complete")
    
    async def initialize_trend_scores_from_metadata(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Initialize trend scores for all datasets using available metadata.
        This is useful when there's no historical metrics_daily data.
        
        Uses:
        - Downloads count (normalized)
        - Likes/stars count (normalized)
        - Recency (newer datasets score higher)
        - Has intelligence (bonus for analyzed datasets)
        
        Returns:
            Dictionary with update statistics
        """
        logger.info("Initializing trend scores from metadata...")
        
        # Get all datasets
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 100000)
        
        if not datasets:
            return {'status': 'no_datasets', 'updated': 0}
        
        # Calculate global max values for normalization
        max_downloads = 1
        max_likes = 1
        
        for d in datasets:
            metadata = d.get('source', {}).get('source_metadata', {})
            downloads = metadata.get('downloads', 0) or 0
            likes = metadata.get('likes', 0) or 0
            max_downloads = max(max_downloads, downloads)
            max_likes = max(max_likes, likes)
        
        logger.info(f"Normalization: max_downloads={max_downloads}, max_likes={max_likes}")
        
        updated_count = 0
        
        for dataset in datasets:
            try:
                metadata = dataset.get('source', {}).get('source_metadata', {})
                
                # Get raw values
                downloads = metadata.get('downloads', 0) or 0
                likes = metadata.get('likes', 0) or 0
                
                # Normalize to 0-1 range using log scale for better distribution
                downloads_score = np.log1p(downloads) / np.log1p(max_downloads) if max_downloads > 0 else 0
                likes_score = np.log1p(likes) / np.log1p(max_likes) if max_likes > 0 else 0
                
                # Calculate recency score (newer = higher)
                created_at = dataset.get('created_at') or dataset.get('scraped_at')
                if created_at:
                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        except:
                            created_at = datetime.utcnow()
                    days_old = (datetime.utcnow() - created_at.replace(tzinfo=None)).days
                    recency_score = max(0, 1 - (days_old / 365))  # Decay over 1 year
                else:
                    recency_score = 0.5  # Default for unknown age
                
                # Bonus for datasets with intelligence analysis
                has_intelligence = 1.0 if dataset.get('intelligence') else 0.0
                
                # Bonus for datasets with descriptions
                has_description = 0.8 if len(dataset.get('description', '')) > 50 else 0.3
                
                # Calculate weighted trend score
                # Weights: downloads=0.35, likes=0.25, recency=0.20, intelligence=0.10, description=0.10
                trend_score = (
                    downloads_score * 0.35 +
                    likes_score * 0.25 +
                    recency_score * 0.20 +
                    has_intelligence * 0.10 +
                    has_description * 0.10
                )
                
                # Ensure score is between 0 and 1
                trend_score = max(0.0, min(1.0, float(trend_score)))
                
                # Update dataset
                await self.db.datasets.update_one(
                    {'_id': dataset['_id']},
                    {
                        '$set': {
                            'trend_score': trend_score,
                            'trend_score_updated_at': datetime.utcnow()
                        }
                    }
                )
                
                updated_count += 1
                
                if updated_count % 500 == 0:
                    logger.info(f"Updated {updated_count} datasets...")
                
            except Exception as e:
                logger.error(f"Error updating trend score for {dataset.get('_id')}: {e}")
        
        logger.info(f"Trend score initialization complete: {updated_count} datasets updated")
        
        return {
            'status': 'success',
            'updated': updated_count,
            'total': len(datasets),
            'max_downloads': max_downloads,
            'max_likes': max_likes
        }


"""
Time-series forecasting for trend prediction.
Uses Prophet, ARIMA, and ensemble methods.
"""
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class TrendForecaster:
    """Time-series forecasting for dataset trends."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
    
    async def get_time_series_data(
        self,
        dataset_id: ObjectId,
        metric: str = 'downloads',
        days: int = 90
    ) -> pd.DataFrame:
        """
        Get time-series data for a dataset.
        
        Args:
            dataset_id: Dataset ID
            metric: Metric to forecast
            days: Number of days of historical data
            
        Returns:
            DataFrame with date and metric columns
        """
        start_date = datetime.utcnow() - timedelta(days=days)
        
        cursor = self.db.metrics_daily.find({
            'dataset_id': dataset_id,
            'date': {'$gte': start_date}
        }).sort('date', 1)
        
        metrics = await cursor.to_list(length=days)
        
        if not metrics:
            return pd.DataFrame(columns=['ds', 'y'])
        
        # Convert to DataFrame
        df = pd.DataFrame(metrics)
        
        # Prepare for Prophet (requires 'ds' and 'y' columns)
        if metric in df.columns:
            df_prophet = pd.DataFrame({
                'ds': pd.to_datetime(df['date']),
                'y': df[metric].fillna(0)
            })
        else:
            df_prophet = pd.DataFrame(columns=['ds', 'y'])
        
        return df_prophet
    
    def forecast_with_prophet(
        self,
        df: pd.DataFrame,
        periods: int = 30,
        confidence_interval: float = 0.95
    ) -> Tuple[pd.DataFrame, Prophet]:
        """
        Forecast using Prophet.
        
        Args:
            df: DataFrame with 'ds' and 'y' columns
            periods: Number of periods to forecast
            confidence_interval: Confidence interval width
            
        Returns:
            Tuple of (forecast DataFrame, model)
        """
        if len(df) < 2:
            logger.warning("Insufficient data for Prophet forecasting")
            return pd.DataFrame(), None
        
        try:
            # Initialize Prophet
            model = Prophet(
                interval_width=confidence_interval,
                daily_seasonality=False,
                weekly_seasonality=True,
                yearly_seasonality=False
            )
            
            # Fit model
            model.fit(df)
            
            # Make future dataframe
            future = model.make_future_dataframe(periods=periods)
            
            # Forecast
            forecast = model.predict(future)
            
            return forecast, model
            
        except Exception as e:
            logger.error(f"Prophet forecasting error: {e}")
            return pd.DataFrame(), None
    
    def forecast_with_arima(
        self,
        df: pd.DataFrame,
        periods: int = 30,
        order: Tuple[int, int, int] = (1, 1, 1)
    ) -> Optional[np.ndarray]:
        """
        Forecast using ARIMA.
        
        Args:
            df: DataFrame with time series data
            periods: Number of periods to forecast
            order: ARIMA order (p, d, q)
            
        Returns:
            Array of forecasted values
        """
        if len(df) < 10:
            logger.warning("Insufficient data for ARIMA forecasting")
            return None
        
        try:
            # Fit ARIMA model
            model = ARIMA(df['y'].values, order=order)
            fitted = model.fit()
            
            # Forecast
            forecast = fitted.forecast(steps=periods)
            
            return forecast
            
        except Exception as e:
            logger.error(f"ARIMA forecasting error: {e}")
            return None
    
    def forecast_with_random_forest(
        self,
        df: pd.DataFrame,
        periods: int = 30,
        lag_features: int = 7
    ) -> Optional[np.ndarray]:
        """
        Forecast using Random Forest with lag features.
        
        Args:
            df: DataFrame with time series data
            periods: Number of periods to forecast
            lag_features: Number of lag features to create
            
        Returns:
            Array of forecasted values
        """
        if len(df) < lag_features + 10:
            logger.warning("Insufficient data for Random Forest forecasting")
            return None
        
        try:
            # Create lag features
            data = df['y'].values
            X, y = [], []
            
            for i in range(lag_features, len(data)):
                X.append(data[i-lag_features:i])
                y.append(data[i])
            
            X = np.array(X)
            y = np.array(y)
            
            # Train Random Forest
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X, y)
            
            # Forecast
            forecast = []
            current_window = data[-lag_features:].tolist()
            
            for _ in range(periods):
                pred = model.predict([current_window])[0]
                forecast.append(pred)
                current_window = current_window[1:] + [pred]
            
            return np.array(forecast)
            
        except Exception as e:
            logger.error(f"Random Forest forecasting error: {e}")
            return None
    
    async def ensemble_forecast(
        self,
        dataset_id: ObjectId,
        metric: str = 'downloads',
        periods: int = 30,
        weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        Ensemble forecast combining multiple methods.
        
        Args:
            dataset_id: Dataset ID
            metric: Metric to forecast
            periods: Number of periods to forecast
            weights: Weights for each method
            
        Returns:
            Dictionary with forecast results
        """
        if weights is None:
            weights = {
                'prophet': 0.5,
                'arima': 0.3,
                'random_forest': 0.2
            }
        
        # Get historical data
        df = await self.get_time_series_data(dataset_id, metric)
        
        if len(df) < 10:
            logger.warning(f"Insufficient data for forecasting dataset {dataset_id}")
            return {
                'forecast': [],
                'confidence_lower': [],
                'confidence_upper': [],
                'method': 'insufficient_data'
            }
        
        forecasts = {}
        
        # Prophet forecast
        prophet_forecast, prophet_model = self.forecast_with_prophet(df, periods)
        if not prophet_forecast.empty:
            forecasts['prophet'] = prophet_forecast['yhat'].tail(periods).values
            confidence_lower = prophet_forecast['yhat_lower'].tail(periods).values
            confidence_upper = prophet_forecast['yhat_upper'].tail(periods).values
        
        # ARIMA forecast
        arima_forecast = self.forecast_with_arima(df, periods)
        if arima_forecast is not None:
            forecasts['arima'] = arima_forecast
        
        # Random Forest forecast
        rf_forecast = self.forecast_with_random_forest(df, periods)
        if rf_forecast is not None:
            forecasts['random_forest'] = rf_forecast
        
        # Ensemble forecast
        if forecasts:
            ensemble = np.zeros(periods)
            total_weight = 0
            
            for method, forecast in forecasts.items():
                if method in weights:
                    ensemble += forecast * weights[method]
                    total_weight += weights[method]
            
            if total_weight > 0:
                ensemble /= total_weight
            
            # Calculate confidence intervals (use Prophet's if available)
            if 'prophet' in forecasts:
                result = {
                    'forecast': ensemble.tolist(),
                    'confidence_lower': confidence_lower.tolist(),
                    'confidence_upper': confidence_upper.tolist(),
                    'method': 'ensemble',
                    'methods_used': list(forecasts.keys())
                }
            else:
                # Estimate confidence intervals as ±20% of forecast
                result = {
                    'forecast': ensemble.tolist(),
                    'confidence_lower': (ensemble * 0.8).tolist(),
                    'confidence_upper': (ensemble * 1.2).tolist(),
                    'method': 'ensemble',
                    'methods_used': list(forecasts.keys())
                }
            
            return result
        
        return {
            'forecast': [],
            'confidence_lower': [],
            'confidence_upper': [],
            'method': 'failed'
        }
    
    async def detect_change_points(
        self,
        dataset_id: ObjectId,
        metric: str = 'downloads'
    ) -> List[Dict[str, Any]]:
        """
        Detect change points in growth patterns.
        
        Args:
            dataset_id: Dataset ID
            metric: Metric to analyze
            
        Returns:
            List of detected change points
        """
        df = await self.get_time_series_data(dataset_id, metric, days=180)
        
        if len(df) < 30:
            return []
        
        change_points = []
        
        # Calculate rolling statistics
        window = 7
        df['rolling_mean'] = df['y'].rolling(window=window).mean()
        df['rolling_std'] = df['y'].rolling(window=window).std()
        
        # Detect significant changes
        for i in range(window, len(df) - window):
            current_mean = df['rolling_mean'].iloc[i]
            prev_mean = df['rolling_mean'].iloc[i - window]
            current_std = df['rolling_std'].iloc[i]
            
            if current_std > 0:
                # Z-score of change
                z_score = abs(current_mean - prev_mean) / current_std
                
                if z_score > 2:  # Significant change (2 standard deviations)
                    change_points.append({
                        'date': df['ds'].iloc[i],
                        'value': df['y'].iloc[i],
                        'change_magnitude': current_mean - prev_mean,
                        'z_score': z_score
                    })
        
        return change_points
    
    async def save_predictions(
        self,
        dataset_id: ObjectId,
        forecast_result: Dict[str, Any],
        metric: str = 'trend_score'
    ):
        """
        Save predictions to database.
        
        Args:
            dataset_id: Dataset ID
            forecast_result: Forecast results
            metric: Metric being forecasted
        """
        if not forecast_result.get('forecast'):
            return
        
        predictions = []
        base_date = datetime.utcnow()
        
        for i, (pred, lower, upper) in enumerate(zip(
            forecast_result['forecast'],
            forecast_result['confidence_lower'],
            forecast_result['confidence_upper']
        )):
            prediction_date = base_date + timedelta(days=i+1)
            
            predictions.append({
                'dataset_id': dataset_id,
                'prediction_date': prediction_date,
                'predicted_score': float(pred),
                'confidence_lower': float(lower),
                'confidence_upper': float(upper),
                'model_type': forecast_result['method'],
                'metric': metric,
                'created_at': datetime.utcnow()
            })
        
        if predictions:
            await self.db.predictions.insert_many(predictions)
            logger.info(f"Saved {len(predictions)} predictions for dataset {dataset_id}")
    
    async def forecast_all_datasets(
        self,
        periods: int = 30,
        limit: Optional[int] = None
    ):
        """
        Generate forecasts for all datasets.
        
        Args:
            periods: Number of periods to forecast
            limit: Optional limit on number of datasets
        """
        logger.info("Starting forecasting for all datasets")
        
        # Get datasets with sufficient data
        cursor = self.db.datasets.find({})
        if limit:
            cursor = cursor.limit(limit)
        
        datasets = await cursor.to_list(length=limit or 10000)
        
        forecasted_count = 0
        
        for dataset in datasets:
            try:
                # Generate forecast
                forecast = await self.ensemble_forecast(
                    dataset['_id'],
                    metric='downloads',
                    periods=periods
                )
                
                if forecast['forecast']:
                    # Save predictions
                    await self.save_predictions(dataset['_id'], forecast)
                    forecasted_count += 1
                
                if forecasted_count % 50 == 0:
                    logger.info(f"Forecasted {forecasted_count} datasets")
                
            except Exception as e:
                logger.error(f"Error forecasting dataset {dataset['_id']}: {e}")
        
        logger.info(f"Forecasting complete: {forecasted_count} datasets")
        
        return forecasted_count

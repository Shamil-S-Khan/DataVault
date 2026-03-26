"""ML tasks - placeholder for Phase 2."""
from celery import shared_task

@shared_task
def train_models():
    """Train ML models (Phase 2)."""
    return {'status': 'Phase 2 - not yet implemented'}

@shared_task
def generate_predictions():
    """Generate and save trend predictions for all datasets."""
    import asyncio
    from app.db.connection import mongodb
    from app.analytics.forecasting import TrendForecaster
    
    async def run_forecast():
        # Connect to DB if not already connected
        if not hasattr(mongodb, 'db') or mongodb.db is None:
            await mongodb.connect()
            
        forecaster = TrendForecaster(mongodb.db)
        # forecast_all_datasets reads metrics_daily and writes to predictions collection
        count = await forecaster.forecast_all_datasets()
        return count

    try:
        count = asyncio.run(run_forecast())
        logger.info(f"Generated predictions for {count} datasets")
        return {'status': 'success', 'forecasted_count': count}
    except Exception as e:
        logger.error(f"Prediction generation failed: {e}")
        return {'status': 'error', 'message': str(e)}

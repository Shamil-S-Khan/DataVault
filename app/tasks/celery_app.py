"""
Celery application configuration.
Task queue for async processing with Redis broker.
"""
from celery import Celery
from celery.schedules import crontab
from app.config import settings

# Create Celery app
celery_app = Celery(
    'datavault',
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        'app.tasks.scraping_tasks',
        'app.tasks.ml_tasks',
        'app.tasks.llm_tasks'
    ]
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=60 * 60,  # 60 minutes for large batch operations
    task_soft_time_limit=55 * 60,  # 55 minutes soft limit
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
)

# Celery Beat schedule
celery_app.conf.beat_schedule = {
    'daily-scraping': {
        'task': 'app.tasks.scraping_tasks.run_daily_scraping',
        'schedule': crontab(hour=2, minute=0),  # 2 AM UTC daily
    },
    'weekly-ml-training': {
        'task': 'app.tasks.ml_tasks.train_models',
        'schedule': crontab(hour=3, minute=0, day_of_week=0),  # Sunday 3 AM UTC
    },
    'weekly-predictions': {
        'task': 'app.tasks.ml_tasks.generate_predictions',
        'schedule': crontab(hour=4, minute=0, day_of_week=0),  # Sunday 4 AM UTC
    },
    'daily-metrics-aggregation': {
        'task': 'app.tasks.scraping_tasks.aggregate_metrics',
        'schedule': crontab(hour=1, minute=0),  # 1 AM UTC daily
    },
}

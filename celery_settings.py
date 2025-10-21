from celery import Celery
import os

REDIS_URL = os.getenv("REDIS_URL")

celery_app = Celery(
    "parser_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='Europe/Amsterdam',
    enable_utc=True,
)

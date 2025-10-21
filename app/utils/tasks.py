import asyncio

from ...celery_settings import celery_app

from ..utils import parse as p


@celery_app.task
def parsing_fork_task(data):
    return asyncio.run(p.parsing_fork(data))

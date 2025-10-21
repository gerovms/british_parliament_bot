import asyncio

from celery import shared_task

from ..utils import parse as p


@shared_task
def parsing_fork_task(data):
    return asyncio.run(p.parsing_fork(data))

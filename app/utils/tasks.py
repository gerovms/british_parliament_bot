# app/tasks.py
import asyncio

from aiogram.types import Message
from celery import shared_task

from ..handlers.core import background_parse


@shared_task
def background_parse_task(message: Message, data: dict):
    """
    Celery-обертка для фонового парсинга.
    """
    async def main():
        return await background_parse(message, data)

    return asyncio.run(main())

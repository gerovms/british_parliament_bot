import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot
from aiogram.types import FSInputFile
from celery import Celery
from dotenv import load_dotenv

import app.utils.constants as c
from app.keyboards import keyboards as kb
from app.utils import parse as p
from app.utils.making_file import save_parsed_data

from ..db.db import get_conn
from ..redis.redis_client import get_redis_client, get_redis_queue

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
celery_app = Celery(
    'tasks',
    broker=CELERY_BROKER_URL
)
BOT_TOKEN = os.getenv('TOKEN')


async def parse_and_send(data: dict,
                         parsed_data,
                         filename: str,
                         bot: Bot):
    """Сохраняет файл и отправляет его пользователю."""
    chat_id = data['chat_id']
    user_first_name = data['user_first_name']

    file_path = await save_parsed_data(parsed_data, filename)

    if not os.path.exists(file_path):
        await bot.send_message(
            chat_id,
            'Файл не удалось создать ❌',
            reply_markup=kb.to_main,
        )
        return

    document = FSInputFile(file_path, filename=filename)

    await bot.send_document(
        chat_id,
        document,
        caption='Вот твой файл с результатами 📄',
        reply_markup=kb.to_main,
    )

    logging.info(f'{user_first_name} получил файл')


async def background_parse(data: dict, conn, redis_client, bot):
    """Основная логика фонового парсинга."""
    chat_id = data['chat_id']
    user_first_name = data['user_first_name']
    try:
        result, filename = await p.parsing_fork(data,
                                                conn,
                                                redis_client,
                                                bot)
        await parse_and_send(data, result, filename, bot)
    except Exception as e:
        logging.exception(f'Ошибка при парсинге для {user_first_name}: {e}')
        await bot.send_message(
            chat_id,
            'Произошла ошибка при обработке запроса ❌',
            reply_markup=kb.to_main,
        )


@celery_app.task(name="background_parse")
def background_parse_task(data: dict):
    """Celery-обёртка для запуска асинхронного парсинга."""
    async def _async_task():
        conn = await get_conn()
        bot = Bot(token=BOT_TOKEN)
        redis_client = get_redis_client()
        chat_id = data['chat_id']
        await remove_user_from_queue(chat_id)
        await bot.send_message(
            chat_id,
            '🚀 Ваша очередь подошла! Начинаем обработку…',
            f'Запрос {data['keyword']}, {data['from_date']}, '
            f'{data['to_date']}.'
        )
        try:
            await background_parse(data,
                                   conn,
                                   redis_client,
                                   bot)
        finally:
            await conn.close()
            await bot.session.close()
            await redis_client.close()
            await redis_client.connection_pool.disconnect()

    asyncio.run(_async_task())


async def add_user_to_queue(data: dict) -> int:
    redis = get_redis_queue()
    user_name = data['user_first_name']
    chat_id = data['chat_id']
    await redis.rpush(c.CELERY_QUEUE_TABLE_NAME,
                      f"{chat_id}:{user_name}")
    position = await redis.llen(c.CELERY_QUEUE_TABLE_NAME)
    await redis.close()
    await redis.connection_pool.disconnect()
    return position


async def remove_user_from_queue(chat_id: int):
    redis = get_redis_queue()
    queue = await redis.lrange(c.CELERY_QUEUE_TABLE_NAME,
                               0,
                               -1)
    for item in queue:
        if item.decode().startswith(str(chat_id)):
            await redis.lrem(c.CELERY_QUEUE_TABLE_NAME,
                             1,
                             item)
            break
    await redis.close()
    await redis.connection_pool.disconnect()

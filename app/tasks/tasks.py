import asyncio
import logging
import os
from pathlib import Path

import redis.asyncio as aioredis
from aiogram import Bot
from aiogram.types import FSInputFile
from celery import Celery
from dotenv import load_dotenv

from app.keyboards import keyboards as kb
from app.utils import parse as p
from app.utils.making_file import save_parsed_data

from ..db.db import get_conn


BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
celery_app = Celery(
    "tasks",
    broker=CELERY_BROKER_URL
)

BOT_TOKEN = os.getenv("TOKEN")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))



async def parse_and_send(data: dict,
                         parsed_data,
                         filename: str,
                         bot: Bot):
    """Сохраняет файл и отправляет его пользователю."""
    chat_id = data["chat_id"]
    user_first_name = data["user_first_name"]

    file_path = await save_parsed_data(parsed_data, filename)

    if not os.path.exists(file_path):
        await bot.send_message(
            chat_id,
            "Файл не удалось создать ❌",
            reply_markup=kb.to_main,
        )
        return

    document = FSInputFile(file_path, filename=filename)

    await bot.send_document(
        chat_id,
        document,
        caption="Вот твой файл с результатами 📄",
        reply_markup=kb.to_main,
    )

    logging.info(f"{user_first_name} получил файл")


async def background_parse(data: dict, redis_client, bot):
    """Основная логика фонового парсинга."""
    chat_id = data["chat_id"]
    user_first_name = data["user_first_name"]
    conn = await get_conn()
    try:
        result, filename = await p.parsing_fork(data,
                                                conn,
                                                redis_client,
                                                bot)
        await parse_and_send(data, result, filename, bot)
    except Exception as e:
        logging.exception(f"Ошибка при парсинге для {user_first_name}: {e}")
        await bot.send_message(
            chat_id,
            "Произошла ошибка при обработке запроса ❌",
            reply_markup=kb.to_main,
        )
    finally:
        await conn.close()
        await redis_client.close()


@celery_app.task(name="background_parse")
def background_parse_task(data: dict):
    """Celery-обёртка для запуска асинхронного парсинга."""
    bot = Bot(token=BOT_TOKEN)
    redis_client = aioredis.Redis(host=REDIS_HOST,
                                  port=REDIS_PORT,
                                  db=REDIS_DB)
    asyncio.run(background_parse(data, redis_client, bot))

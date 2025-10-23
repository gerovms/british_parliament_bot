import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot
from aiogram.types import FSInputFile
from celery import Celery
from dotenv import load_dotenv

from app.keyboards import keyboards as kb
from app.utils import parse as p
from app.utils.making_file import save_parsed_data


BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
celery_app = Celery(
    "tasks",
    broker=CELERY_BROKER_URL
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)


async def parse_and_send(data: dict, parsed_data, filename: str):
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


async def background_parse(data: dict):
    """Основная логика фонового парсинга."""
    chat_id = data["chat_id"]

    try:
        result, filename = await p.parsing_fork(data)
        await parse_and_send(data, result, filename)
    except Exception as e:
        logging.exception(f"Ошибка при парсинге: {e}")
        await bot.send_message(
            chat_id,
            "Произошла ошибка при обработке запроса ❌",
            reply_markup=kb.to_main,
        )


@celery_app.task(name="background_parse")
def background_parse_task(data: dict):
    """Celery-обёртка для запуска асинхронного парсинга."""
    asyncio.run(background_parse(data))

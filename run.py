import asyncio
import logging
import os
from datetime import datetime
from os import getenv

from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

from app.handlers import router
from app.db.db import init_db

load_dotenv()

TOKEN = getenv("TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


RESULTS_DIR = os.path.join(os.getcwd(), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

dp = Dispatcher()
dp.include_router(router=router)


async def cleanup_results_folder():
    """Фоновая задача: удаляет все файлы из папки results раз в сутки."""
    while True:
        if os.path.exists(RESULTS_DIR):
            for filename in os.listdir(RESULTS_DIR):
                file_path = os.path.join(RESULTS_DIR, filename)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"[{datetime.now()}] Удалён файл: {filename}")
                except Exception as e:
                    print(f"[{datetime.now()}] Ошибка при удалении {filename}: {e}")
        await asyncio.sleep(24 * 60 * 60)


async def main() -> None:
    await init_db()
    bot = Bot(token=TOKEN)
    print(f"[{datetime.now()}] Бот запущен")

    asyncio.create_task(cleanup_results_folder())

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        print(f"[{datetime.now()}] Бот остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Бот выключен вручную')

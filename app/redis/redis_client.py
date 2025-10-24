import os
from pathlib import Path

import redis.asyncio as aioredis
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB_PAGES = int(os.getenv("REDIS_DB_PAGES"))


def get_redis_client():
    return aioredis.Redis(host=REDIS_HOST,
                          port=REDIS_PORT,
                          db=REDIS_DB_PAGES)

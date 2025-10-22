import asyncio
import logging
import os

import asyncpg

from .constants import DELAY, RETRIES

DB_URL = os.getenv("DATABASE_URL")


async def get_conn():
    for i in range(RETRIES):
        try:
            return await asyncpg.connect(DB_URL)
        except Exception as e:
            if i == RETRIES - 1:
                raise
            logging.error(f"[DB] Failed to connect "
                          f"(attempt {i+1}/{RETRIES}): {e}")
            await asyncio.sleep(DELAY)


async def init_db():
    conn = await get_conn()
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE,
        content TEXT
    );
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_documents_url ON documents (url);
    """)
    await conn.close()


async def save_document(url: str, content: str):
    conn = await get_conn()
    try:
        await conn.execute(
            ("INSERT INTO documents (url, content) VALUES ($1, $2) "
             "ON CONFLICT (url) DO NOTHING"),
            url,
            content
            )
    finally:
        await conn.close()


async def get_document(url: str):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT content FROM documents WHERE url = $1",
            url
        )
        if row is not None:
            return row['content']
        else:
            return False
    finally:
        await conn.close()

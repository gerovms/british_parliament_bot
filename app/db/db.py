# db.py
import asyncpg
import os


DB_URL = os.getenv("DATABASE_URL")


async def get_conn():
    return await asyncpg.connect(DB_URL)


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


async def save_get_document(url: str, content: str):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT content FROM documents WHERE url = $1",
            url
        )
        if row:
            return row
        else:
            await conn.execute(
                "INSERT INTO documents (url, content) VALUES ($1, $2)",
                url,
                content
                )
            return content
    finally:
        await conn.close()

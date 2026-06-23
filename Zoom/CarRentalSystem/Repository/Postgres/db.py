import asyncio
import json
import os
import asyncpg

_pool: asyncpg.Pool | None = None

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://localhost/carrental"
)


async def _init_conn(conn: asyncpg.Connection) -> None:
    # asyncpg returns json/jsonb columns as raw strings by default.
    # Register codecs so they come back as Python dicts/lists automatically.
    await conn.set_type_codec("json",  encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    for attempt in range(1, 31):
        try:
            _pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10, init=_init_conn)
            print(f"[db] Connected to PostgreSQL")
            return _pool
        except Exception as e:
            wait = min(2 ** (attempt - 1), 30)
            print(f"[db] PostgreSQL not ready (attempt {attempt}/30): {e.__class__.__name__} — retrying in {wait}s")
            await asyncio.sleep(wait)
    raise RuntimeError("Could not connect to PostgreSQL after 30 attempts")


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
import asyncio
import json
import os
import asyncpg

_pool: asyncpg.Pool | None = None
_read_pool: asyncpg.Pool | None = None

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://localhost/carrental"
)

# Falls back to the primary when unset, so BookingService/PaymentService/
# IngestionService (which never call get_read_pool()) are unaffected, and
# TicketService still works against a single DB in local/dev setups that
# don't run postgres-replica.
DATABASE_URL_REPLICA = os.getenv("DATABASE_URL_REPLICA", DATABASE_URL)


async def _init_conn(conn: asyncpg.Connection) -> None:
    # asyncpg returns json/jsonb columns as raw strings by default.
    # Register codecs so they come back as Python dicts/lists automatically.
    await conn.set_type_codec("json",  encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def _create_pool(dsn: str, label: str) -> asyncpg.Pool:
    for attempt in range(1, 31):
        try:
            pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=2,
                max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
                init=_init_conn,
            )
            print(f"[db] Connected to PostgreSQL ({label})")
            return pool
        except Exception as e:
            wait = min(2 ** (attempt - 1), 30)
            print(f"[db] PostgreSQL ({label}) not ready (attempt {attempt}/30): {e.__class__.__name__} — retrying in {wait}s")
            await asyncio.sleep(wait)
    raise RuntimeError(f"Could not connect to PostgreSQL ({label}) after 30 attempts")


async def get_pool() -> asyncpg.Pool:
    """Primary (read-write) pool — every write, and every read that must be
    strongly consistent with a write that just happened, goes through this."""
    global _pool
    if _pool is None:
        _pool = await _create_pool(DATABASE_URL, "primary")
    return _pool


async def get_read_pool() -> asyncpg.Pool:
    """Read replica pool — only for reads that can tolerate a few hundred ms
    of replication lag (e.g. status-polling GET endpoints). Never write through
    this pool: a hot standby rejects writes at the Postgres level anyway."""
    global _read_pool
    if _read_pool is None:
        _read_pool = await _create_pool(DATABASE_URL_REPLICA, "read-replica")
    return _read_pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def close_read_pool() -> None:
    global _read_pool
    if _read_pool is not None:
        await _read_pool.close()
        _read_pool = None
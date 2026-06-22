import asyncio
import asyncpg
from typing import Any

_BATCH_SIZE = 100
_POLL_INTERVAL = 1.0  # seconds


class OutboxRelay:
    # event_handler: any object with publish_raw(topic: str, payload: dict)
    # Works with both KafkaEventHandler and the legacy HTTP EventHandler.
    def __init__(self, pool: asyncpg.Pool, event_handler: Any):
        self._pool = pool
        self._event_handler = event_handler

    async def run(self) -> None:
        print("[OutboxRelay] Started polling outbox...")
        while True:
            try:
                published = await self._process_batch()
                if published == 0:
                    await asyncio.sleep(_POLL_INTERVAL)
            except Exception as e:
                print(f"[OutboxRelay] Error: {e} — retrying in {_POLL_INTERVAL}s")
                await asyncio.sleep(_POLL_INTERVAL)

    async def _process_batch(self) -> int:
        """
        Fetch up to _BATCH_SIZE unpublished rows, publish each, mark published.
        FOR UPDATE SKIP LOCKED means multiple relay instances won't step on each other.
        Returns number of rows published.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, topic, payload
                    FROM outbox
                    WHERE published = false
                    ORDER BY id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                    _BATCH_SIZE,
                )

                for row in rows:
                    await self._event_handler.publish_raw(row["topic"], dict(row["payload"]))
                    await conn.execute(
                        "UPDATE outbox SET published = true WHERE id = $1",
                        row["id"],
                    )

        return len(rows)

import asyncio
import json
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
        Fetch unpublished rows, publish to Kafka, then mark published.

        Kafka I/O is intentionally outside any Postgres transaction.
        Holding a FOR UPDATE lock while waiting on send_and_wait (which
        can block for seconds with acks=all or during broker rebalancing)
        prevents every other relay instance from seeing the row via SKIP LOCKED,
        effectively stalling the relay indefinitely.

        Trade-off: at-least-once Kafka delivery — two relay instances may race
        and both publish the same row. Downstream consumers guard against this
        with idempotent DB writes (ON CONFLICT DO NOTHING).
        """
        # Step 1: fetch without holding a transaction or lock
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, topic, payload
                FROM outbox
                WHERE published = false
                ORDER BY id
                LIMIT $1
                """,
                _BATCH_SIZE,
            )

        if not rows:
            return 0

        # Step 2: publish to Kafka, then mark published with a CAS guard.
        # WHERE published = false ensures only the first writer wins if two
        # relay instances race on the same row.
        published_count = 0
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            await self._event_handler.publish_raw(row["topic"], payload)
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE outbox SET published = true WHERE id = $1 AND published = false",
                    row["id"],
                )
            if result == "UPDATE 1":
                published_count += 1
                print(f"[OutboxRelay] Published row {row['id']} → {row['topic']}")

        return published_count

import asyncio
import json
import os
import asyncpg
from typing import Any

from Zoom.CarRentalSystem.metrics import outbox_relay_published_total, outbox_relay_batch_size

_BATCH_SIZE    = int(os.getenv("OUTBOX_BATCH_SIZE",    "100"))
_POLL_INTERVAL = float(os.getenv("OUTBOX_POLL_INTERVAL", "1.0"))  # seconds — sleep only when queue is empty
# How many rows to publish concurrently within one batch. Previously every row
# was sent with `await publish_raw(...)` one at a time — each send blocked on
# its own network round trip before the next one even started. aiokafka's
# producer already accumulates concurrent sends into fewer, larger requests to
# the broker (see linger_ms in kafka_event_handler.py); dispatching them
# concurrently here is what lets that batching actually kick in instead of
# forcing one-message-per-request regardless of producer config.
_PUBLISH_CONCURRENCY = int(os.getenv("OUTBOX_PUBLISH_CONCURRENCY", "50"))


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
        Fetch unpublished rows, publish to Kafka concurrently, then mark published.

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

        # Step 2: publish to Kafka concurrently (bounded by a semaphore so one
        # relay instance can't open hundreds of simultaneous producer sends),
        # then mark each row published with a CAS guard. WHERE published = false
        # ensures only the first writer wins if two relay instances race on the
        # same row.
        sem = asyncio.Semaphore(_PUBLISH_CONCURRENCY)

        async def _publish_and_mark(row) -> bool:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            async with sem:
                await self._event_handler.publish_raw(row["topic"], payload)
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE outbox SET published = true WHERE id = $1 AND published = false",
                    row["id"],
                )
            ok = result == "UPDATE 1"
            if ok:
                print(f"[OutboxRelay] Published row {row['id']} → {row['topic']}")
            return ok

        results = await asyncio.gather(*(_publish_and_mark(row) for row in rows))
        published_count = sum(1 for ok in results if ok)

        outbox_relay_published_total.inc(published_count)
        outbox_relay_batch_size.observe(published_count)
        return published_count

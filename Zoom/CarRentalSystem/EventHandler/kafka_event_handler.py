import asyncio
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class KafkaEventHandler:
    """
    Kafka primer — concepts used here:

    PRODUCER SIDE
    ─────────────
    acks="all"
        Every message must be acknowledged by ALL in-sync replicas before
        send_and_wait() returns. Default is acks=1 (leader only).
        acks=0  → fire-and-forget, fastest, no durability guarantee
        acks=1  → leader writes to disk, then acks — leader crash = message lost
        acks=all → all in-sync replicas confirm — survives a broker crash

    enable_idempotence=True
        Without this, if the producer retries a timed-out send it can write
        the same message twice (the broker never knew the first one arrived).
        With idempotence each message gets a sequence number; the broker
        deduplicates retries automatically. Requires acks=all.

    CONSUMER SIDE
    ─────────────
    Offset: Kafka remembers WHERE in a topic your consumer group stopped.
    That position is called the "offset". By default Kafka auto-commits it
    every 5 seconds (enable_auto_commit=True). Problem:

        read msg → [5 s auto-commit fires] → crash before processing
        → offset already advanced → message is LOST forever

    enable_auto_commit=False + manual commit_offset()
        We commit only AFTER the message has been fully processed (written to DB).
        If we crash between read and commit, Kafka redelivers the message on
        reconnect — "at-least-once" delivery. Your processing must be idempotent
        (the DB INSERT … ON CONFLICT DO NOTHING already handles this).

    Consumer group: all instances sharing the same group_id share the load.
    auto_offset_reset="latest": on first connect, skip historical messages.
    """

    def __init__(self, bootstrap_servers: str, group_id: str):
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._producer: AIOKafkaProducer | None = None
        self._consumers: dict[str, AIOKafkaConsumer] = {}

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Wait for ALL in-sync replicas before returning — survives broker crash.
            acks="all",
            # Broker deduplicates retries using sequence numbers.
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()
        for consumer in self._consumers.values():
            await consumer.stop()

    async def add(self, topic, event) -> None:
        """
        Produce an event. send_and_wait() blocks until acks=all is satisfied,
        so by the time this returns, the message is durable on the broker.
        """
        await self._producer.send_and_wait(
            topic.value,
            event.model_dump(mode="json"),
        )

    async def get_tasks(self, topic_str: str) -> dict:
        """
        Consume the next message. Consumer created lazily and reused.
        Offset is NOT committed here — call commit_offset() after you have
        safely processed the message.
        """
        if topic_str not in self._consumers:
            consumer = AIOKafkaConsumer(
                topic_str,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                # Never auto-advance the offset; we commit explicitly after DB write.
                enable_auto_commit=False,
            )
            await consumer.start()
            self._consumers[topic_str] = consumer

        msg = await self._consumers[topic_str].getone()
        return msg.value

    async def publish_raw(self, topic: str, payload: dict) -> None:
        """
        Used by OutboxRelay — publishes a pre-built dict (already the correct
        event shape) directly to Kafka without going through a Pydantic model.
        The value_serializer handles JSON encoding.
        """
        await self._producer.send_and_wait(topic, payload)

    async def process_with_dlq(
        self,
        topic_str: str,
        dlq_topic_str: str,
        handler,
        max_retries: int = 3,
    ) -> None:
        """
        Reads one message from topic_str, runs handler(raw_dict).

        Why this exists — the poison-pill problem:
          Without a DLQ, a malformed or persistently-failing message causes the
          consumer to retry forever, blocking every message behind it in that
          partition. Nothing processes until someone manually intervenes.

        What this does instead:
          Attempt 1      → handler fails → wait 1s
          Attempt 2      → handler fails → wait 2s
          Attempt 3      → handler fails → publish to DLQ + commit offset → move on
          Attempt N (ok) → commit offset → done

        The DLQ topic holds the original payload + error for ops to inspect and
        replay once the underlying bug is fixed. The live partition is never blocked.

        Exponential backoff (1s, 2s, 4s…) handles transient failures (DB blip,
        downstream timeout) without hammering a struggling dependency.
        """
        raw = await self.get_tasks(topic_str)
        last_exc: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                await handler(raw)
                await self.commit_offset(topic_str)
                return
            except Exception as exc:
                last_exc = exc
                wait = 2 ** (attempt - 1)   # 1s, 2s, 4s
                print(f"[DLQ] {topic_str} attempt {attempt}/{max_retries} failed: {exc}"
                      + (f" — retrying in {wait}s" if attempt < max_retries else ""))
                if attempt < max_retries:
                    await asyncio.sleep(wait)

        # All retries exhausted — dead-letter the message.
        await self.publish_raw(dlq_topic_str, {
            "original_topic": topic_str,
            "error": str(last_exc),
            "payload": raw,
        })
        await self.commit_offset(topic_str)
        print(f"[DLQ] Moved to {dlq_topic_str}  error={last_exc}")

    async def commit_offset(self, topic_str: str) -> None:
        """
        Tell Kafka "I have processed everything up to and including the last
        message I read on this topic." Call this only AFTER your side-effects
        (DB write, etc.) have committed successfully.

        If you crash before calling this, Kafka redelivers — that's fine because
        your DB operation is idempotent (ON CONFLICT DO NOTHING).
        """
        if topic_str in self._consumers:
            await self._consumers[topic_str].commit()

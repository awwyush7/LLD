import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class KafkaEventHandler:
    """
    Drop-in replacement for EventHandler that uses Kafka instead of the
    custom HTTP-based EventStreamer.

    Kafka concepts at play here:
    - Producer: sends (produces) messages to a topic
    - Consumer: reads (consumes) messages from a topic
    - Consumer group: all instances sharing the same group_id split messages
      across themselves — so each service type gets every message exactly once
    - auto_offset_reset="latest": on first connect, start from new messages
      only (matches the old in-memory queue behaviour; use "earliest" to
      replay from the beginning of the topic)
    - getone(): blocks until the next message arrives, then returns it —
      this is how the while-True consumer loops in each service work
    """

    def __init__(self, bootstrap_servers: str, group_id: str):
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._producer: AIOKafkaProducer | None = None
        # One AIOKafkaConsumer per topic string, created lazily on first get_tasks call
        self._consumers: dict[str, AIOKafkaConsumer] = {}

    async def start(self):
        """Start the producer. Call this once before add() or get_tasks()."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()

    async def stop(self):
        """Gracefully shut down producer and all open consumers."""
        if self._producer:
            await self._producer.stop()
        for consumer in self._consumers.values():
            await consumer.stop()

    async def add(self, topic, event) -> None:
        """
        Produce an event to a Kafka topic.
        `topic` is the Topic enum; `event` is any Pydantic Event model.
        send_and_wait blocks until the broker acknowledges the message.
        """
        await self._producer.send_and_wait(
            topic.value,
            event.model_dump(mode="json"),
        )

    async def get_tasks(self, topic_str: str) -> dict:
        """
        Consume the next message from `topic_str`.
        A consumer for this topic is created on the first call and reused
        for every subsequent call — the caller's while-True loop drives it.
        Returns the deserialized dict (same shape as before).
        """
        if topic_str not in self._consumers:
            consumer = AIOKafkaConsumer(
                topic_str,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            await consumer.start()
            self._consumers[topic_str] = consumer

        msg = await self._consumers[topic_str].getone()
        return msg.value

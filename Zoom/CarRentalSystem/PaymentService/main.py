from dotenv import load_dotenv
load_dotenv()

import asyncio
import os
from pydantic import TypeAdapter
from redis.asyncio import Redis

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.PaymentService.payment_service import PaymentService
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.Repository.Postgres.postgres_payment_intent_repository import PostgresPaymentIntentRepository
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Topic.topic import Topic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-service-group")
STRIPE_URL = os.getenv("STRIPE_URL", "http://localhost:8003")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


async def start():
    event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    await event_handler.start()
    print(f"[PaymentService] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    pool = await get_pool()
    intent_repo = PostgresPaymentIntentRepository(pool)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    payment_service = PaymentService(intent_repo, STRIPE_URL, redis)
    adapter = TypeAdapter(AnyEvent)

    async def handle(raw: dict) -> None:
        event = adapter.validate_python(raw)
        print(f"[PaymentService] Got {event.type}  booking={event.booking_id[:8]}")
        await payment_service.process_payment(event)

    print(f"[PaymentService] Listening on {Topic.PaymentTopic.value}...")
    try:
        while True:
            await event_handler.process_with_dlq(
                Topic.PaymentTopic.value,
                Topic.PaymentTopicDLQ.value,
                handle,
            )
    finally:
        await payment_service.close()
        await event_handler.stop()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(start())

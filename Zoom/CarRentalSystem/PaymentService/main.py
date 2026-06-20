from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os
from pydantic import TypeAdapter

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.PaymentService.payment_service import PaymentService
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Topic.topic import Topic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-service-group")


async def start():
    event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    await event_handler.start()
    print(f"[PaymentService] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    payment_service = PaymentService(event_handler)
    adapter = TypeAdapter(AnyEvent)

    print(f"[PaymentService] Listening on {Topic.PaymentTopic.value}...")
    try:
        while True:
            raw = await event_handler.get_tasks(Topic.PaymentTopic.value)
            event = adapter.validate_python(raw)
            print(f"[PaymentService] Got {event.type}  booking={event.booking_id[:8]}")
            asyncio.create_task(payment_service.process_payment(event))
    finally:
        await event_handler.stop()


if __name__ == "__main__":
    asyncio.run(start())
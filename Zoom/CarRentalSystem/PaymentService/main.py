import asyncio
from pydantic import TypeAdapter

from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.PaymentService.payment_service import PaymentService
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Topic.topic import Topic

STREAMER_URL = "http://127.0.0.1:8000"


async def start():
    event_handler = EventHandler(streamer_url=STREAMER_URL)
    payment_service = PaymentService(event_handler)
    adapter = TypeAdapter(AnyEvent)

    print(f"[PaymentService] listening on {Topic.PaymentTopic.value}...")
    while True:
        raw = await event_handler.get_tasks(Topic.PaymentTopic.value)
        event = adapter.validate_python(raw)
        print(f"[PaymentService] Got {event.type} booking = {event.booking_id[:8]}")
        asyncio.create_task(payment_service.process_payment(event))


if __name__ ==  "__main__":
    asyncio.run(start())
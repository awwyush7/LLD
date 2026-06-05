from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os
import httpx
from pydantic import TypeAdapter

from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.PaymentService.payment_service import PaymentService
# from Zoom.CarRentalSystem.metrics import start_metrics_server
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Topic.topic import Topic

STREAMER_URL = os.getenv("STREAMER_URL", "http://127.0.0.1:8000")
METRICS_PORT = 8003


async def wait_for_streamer(url: str, max_retries: int = 30):
    """Wait for EventStreamer to be ready with exponential backoff"""
    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries):
            try:
                response = await client.get(f"{url}/health", timeout=2.0)
                if response.status_code == 200:
                    print(f"[PaymentService] Connected to EventStreamer at {url}")
                    return
            except (httpx.ConnectError, httpx.TimeoutException):
                wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                print(f"[PaymentService] Waiting for EventStreamer... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
        raise RuntimeError(f"EventStreamer not available at {url} after {max_retries} attempts")


async def start():
    await wait_for_streamer(STREAMER_URL)
    
    event_handler = EventHandler(STREAMER_URL)
    payment_service = PaymentService(event_handler)
    adapter = TypeAdapter(AnyEvent)
    # asyncio.create_task(start_metrics_server(METRICS_PORT))

    print(f"[PaymentService] Listening on {Topic.PaymentTopic.value}...")
    while True:
        raw = await event_handler.get_tasks(Topic.PaymentTopic.value)
        event = adapter.validate_python(raw)
        print(f"[PaymentService] Got {event.type}  booking={event.booking_id[:8]}")
        asyncio.create_task(payment_service.process_payment(event))


if __name__ == "__main__":
    asyncio.run(start())
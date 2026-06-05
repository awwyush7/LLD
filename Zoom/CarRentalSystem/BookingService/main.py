from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os
import httpx

from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.BookingService.booking_service import BookingService
from Zoom.CarRentalSystem.BookingService.orchestrator import Orchestrator
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.Repository.Postgres.postgres_vehicle_repository import PostgresVehicleRepository
from Zoom.CarRentalSystem.Repository.Postgres.postgres_booking_repository import PostgresBookingRepository
from Zoom.CarRentalSystem.metrics import start_metrics_server
from Zoom.EventStreamer.Topic.topic import Topic

STREAMER_URL = os.getenv("STREAMER_URL", "http://127.0.0.1:8000")
METRICS_PORT = 8002

# Vehicles to seed on startup — matches the IDs shown in the UI
_SEED_VEHICLES = [
    (f"V{i:03}", "owner1", f"KA-01-{1000 + i}", 2022, "available")
    for i in range(1, 6)
]


async def wait_for_streamer(url: str, max_retries: int = 30):
    """Wait for EventStreamer to be ready with exponential backoff"""
    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries):
            try:
                response = await client.get(f"{url}/health", timeout=2.0)
                if response.status_code == 200:
                    print(f"[BookingService] Connected to EventStreamer at {url}")
                    return
            except (httpx.ConnectError, httpx.TimeoutException):
                wait_time = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                print(f"[BookingService] Waiting for EventStreamer... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
        raise RuntimeError(f"EventStreamer not available at {url} after {max_retries} attempts")


async def start(orchestrator: Orchestrator):
    print("[BookingService] Listening on BookingTopic...")
    asyncio.create_task(start_metrics_server(METRICS_PORT))
    while True:
        raw_event = await orchestrator.get_task(Topic.BookingTopic.value)
        asyncio.create_task(orchestrator.process(raw_event))


async def main():
    await wait_for_streamer(STREAMER_URL)
    
    pool = await get_pool()

    vehicle_repo = PostgresVehicleRepository(pool)
    for vid, owner, plate, year, status in _SEED_VEHICLES:
        await vehicle_repo.add(vid, owner, plate, year, status)
    print(f"[BookingService] Seeded {len(_SEED_VEHICLES)} vehicles")

    booking_repo = PostgresBookingRepository(pool)
    event_handler = EventHandler(STREAMER_URL)
    booking_service = BookingService(booking_repo, event_handler)
    orchestrator = Orchestrator(booking_service, event_handler)

    try:
        await start(orchestrator)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())

# python3 -m Zoom.CarRentalSystem.BookingService.main
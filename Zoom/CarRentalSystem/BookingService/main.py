from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.BookingService.booking_service import BookingService
from Zoom.CarRentalSystem.BookingService.orchestrator import Orchestrator
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.Repository.Postgres.postgres_vehicle_repository import PostgresVehicleRepository
from Zoom.CarRentalSystem.Repository.Postgres.postgres_booking_repository import PostgresBookingRepository
from Zoom.CarRentalSystem.metrics import start_metrics_server
from Zoom.EventStreamer.Topic.topic import Topic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "booking-service-group")
METRICS_PORT = 8002

# Vehicles to seed on startup — matches the IDs shown in the UI
_SEED_VEHICLES = [
    (f"V{i:03}", "owner1", f"KA-01-{1000 + i}", 2022, "available")
    for i in range(1, 13)
]


async def start(orchestrator: Orchestrator):
    print("[BookingService] Listening on BookingTopic...")
    asyncio.create_task(start_metrics_server(METRICS_PORT))
    while True:
        raw_event = await orchestrator.get_task(Topic.BookingTopic.value)
        asyncio.create_task(orchestrator.process(raw_event))


async def main():
    pool = await get_pool()

    vehicle_repo = PostgresVehicleRepository(pool)
    for vid, owner, plate, year, status in _SEED_VEHICLES:
        await vehicle_repo.add(vid, owner, plate, year, status)
    print(f"[BookingService] Seeded {len(_SEED_VEHICLES)} vehicles")

    booking_repo = PostgresBookingRepository(pool)
    event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    await event_handler.start()
    print(f"[BookingService] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    booking_service = BookingService(booking_repo)
    orchestrator = Orchestrator(booking_service, event_handler)

    try:
        await start(orchestrator)
    finally:
        await event_handler.stop()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())

# python3 -m Zoom.CarRentalSystem.BookingService.main
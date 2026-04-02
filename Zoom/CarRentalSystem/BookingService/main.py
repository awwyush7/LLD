from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.BookingService.booking_service import BookingService
from Zoom.CarRentalSystem.BookingService.orchestrator import Orchestrator
from Zoom.CarRentalSystem.VehicleService.vehicle_service import VehicleService
from Zoom.CarRentalSystem.VehicleService.Vehicle.vehicle import Vehicle
from Zoom.EventStreamer.Topic.topic import Topic
import asyncio

STREAMER_URL= "http://127.0.0.1:8000"


async def start(orchestrator: Orchestrator):
    print("[BookingService] Listening on BookingTopic...")
    while True:
        raw_event = await orchestrator.get_task(Topic.BookingTopic.value)
        asyncio.create_task(orchestrator.process(raw_event))


if __name__ == "__main__":
    event_handler = EventHandler(STREAMER_URL)
    vehicle_service = VehicleService()

    # Seed vehicles - same IDs shown in the UI
    for i in range (1, 6):
        vehicle_service.add_vehicle(
            Vehicle(f"V{i:03}", "owner1", f"KA-01-{1000 + 1}", 2022, "available")
        )
    # for c in  vehicle_service.
    print(f"[BookingService] Seeded {len(vehicle_service._vehicles)} vehicles")
    
    booking_service = BookingService(vehicle_service, event_handler)
    orchestrator = Orchestrator(booking_service, event_handler)
    asyncio.run(start(orchestrator))

# python3 -m Zoom.CarRentalSystem.BookingService.main
from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.BookingService.booking_service import BookingService
from Zoom.CarRentalSystem.BookingService.orchestrator import Orchestrator
import asyncio

async def start(manager : Orchestrator):
    while True:
        message = await EventHandler.get_tasks()
        asyncio.create_task(manager.process(message))
    

if __name__ == "__main__":
    booking_service = BookingService()
    manager = Orchestrator(booking_service)
    asyncio.run(start(manager))

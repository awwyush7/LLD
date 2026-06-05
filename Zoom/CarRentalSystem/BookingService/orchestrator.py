from pydantic import TypeAdapter
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.CarRentalSystem.BookingService.booking_service import BookingService


class Orchestrator:
    def __init__(self, booking_service: BookingService, event_handler):
        self._booking_service = booking_service
        self._event_handler = event_handler
        self._handler = {
            "book": booking_service.book_vehicles,
            "payment_success": booking_service.confirm_booking,
            "payment_failure": booking_service.remove_booking,
        }
        self._adapter = TypeAdapter(AnyEvent)

    async def get_task(self, topic: str):
        return await self._event_handler.get_tasks(topic)

    async def process(self, raw_event: dict):
        event = self._adapter.validate_python(raw_event)
        handler = self._handler.get(event.type)
        print(f"[BookingService] Processing event type={event.type}")
        if handler:
            await handler(event)
        else:
            print(f"[BookingService] No handler for event type: {event.type}")

# Topic - BookingTopic
# Consumed event types:
#   "book"            → book_vehicles  (marks pending, publishes PaymentRequestEvent)
#   "payment_success" → confirm_booking (marks booked, publishes GenerateTicketEvent)
#   "payment_failure" → remove_booking  (frees slots)
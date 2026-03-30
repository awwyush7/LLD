from Zoom.CarRentalSystem.BookingService.booking_service import BookingService

class Orchestrator:
    def __init__(self, booking_service, event_handler):
        self._booking_service = booking_service
        self.event_handler = event_handler
        self._handler = {
            "CanelBooking" : BookingService.remove_booking,
            "FailedPayment" : BookingService.remove_booking,
            "Book" : BookingService.book_vehicles
        }

    async def get_tasks(self):
        return await self.event_handler.get_tasks()
    
    async def process(self,message):
        message_action = message["Action"]
        vehicle_ids = message["vehicle_ids"]
        from_date = message["from_date"]
        to_date = message["to_date"]

        self._handler[message_action](vehicle_ids, from_date, to_date)

# Topic - Booking
# 1) CancelBooking
# 2) SuccessfulPayment
# 3) FailedPayment
# 4) Book

    
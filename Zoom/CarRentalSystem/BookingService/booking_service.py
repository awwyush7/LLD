from typing import Optional, List 
from Zoom.EventStreamer.Topic.topic import Topic


class BookingService:
    def __init__(self, vehicle_service, event_streamer):
        self._vehicle_service = vehicle_service
        self._event_streamer = event_streamer
        self._bookings = {} # (vehicle_id: {date: "pending" | "booked"}}

    async def book_vehicles(self, event) -> Optional[str]:
        """Handles BookEvent: acquire locks →mark pending → release → publish PaymentRequestEvent."""
        print("BOOKING STARTED")
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.correlation_id

        sorted_ids = sorted(vehicle_ids)
        acquired = []
        try:
            for vid in sorted_ids:
                vehicle = self._vehicle_service.get(vid)
                if vehicle is None:
                    print("Vehicle is Nonde")
                if vehicle is None or not vehicle.lock.acquire(blocking=False):
                    return None
                print(f"BOOKING: Lock for {vid} acquired")
                acquired.append(vid)
            
            if not self.vehicles_available(vehicle_ids, from_date, to_date):
                return None
            
            for vid in sorted_ids:
                for i in range(from_date, to_date + 1):
                    self._bookings.setdefault(vid, {})[i] = "pending"
        finally:
            for vid in reversed (acquired):
                self._vehicle_service.get (vid) .lock. release()
        
        print(f"Booking For {vehicle_ids}")

        # Publish OUTSIDE the lock - timeout here no longer holds the lock
        from Zoom. EventStreamer.Event.event import PaymentRequestEvent 
        await self._event_streamer.add(
            Topic.PaymentTopic,
            PaymentRequestEvent(
                correlation_id=booking_id, 
                booking_id=booking_id, 
                user_id=event.user_id,
                amount=100.0, # TODO: calculate from vehicle type + date range 
                vehicle_ids=vehicle_ids, 
                from_date=from_date, 
                to_date=to_date,
            ),
        )
        return booking_id
            
    async def confirm_booking(self, event) -> None:
        """Handles PaymentsuccessEvent: mark booked publish GeneratericketEvent."""
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.booking_id

        sorted_ids = sorted (vehicle_ids)
        acquired = []
        try:
            for vid in sorted_ids:
                vehicle = self._vehicle_service.get(vid)
                if vehicle is None or not vehicle.lock.acquire(blocking=False):
                    return
                acquired.append(vid)
            
            for vid in sorted_ids:
                for i in range(from_date, to_date + 1):
                    self._bookings.setdefault(vid, ())[i] = "booked"
        finally:
            for vid in reversed (acquired):
                self._vehicle_service.get(vid).lock.release()

        from Zoom. EventStreamer.Event. event import GenerateTicketEvent 
        await self._event_streamer.add(
            Topic. TicketTopic,
            GenerateTicketEvent(
                correlation_id = booking_id,
                booking_id=booking_id, 
                user_id=event.user_id, 
                vehicle_ids=vehicle_ids, 
                from_date=from_date,
                to_date = to_date,
            ),
        )

    async def remove_booking(self, event) -> None:
        """Handles CancelBooking / PaymentFailureEvent: free the slots."""
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date

        sorted_ids = sorted(vehicle_ids)
        acquired = []
        try:
            for vid in sorted_ids:
                vehicle = self._vehicle_service.get(vid)
                if vehicle is None or not vehicle.lock.acquire(blocking=False):
                    return 
                acquired.append(vid)

            for vid in sorted_ids:
                slots = self._bookings.get(vid, ())
                for i in range(from_date, to_date + 1):
                    slots.pop(i, None)
        finally:
            for vid in reversed (acquired):
                self._vehicle_service.get(vid).lock.release()
    
    def is_available(self, vehicle_id: str, from_date: int, to_date: int) -> bool:
        slots = self._bookings.get(vehicle_id, {})
        return all (slots.get(i) not in ("pending", "booked") for i in range(from_date, to_date + 1)) 
    
    def vehicles_available(self, vehicle_ids: List[str], from_date: int, to_date: int) -> bool:
        return all(self.is_available(vid, from_date, to_date) for vid in vehicle_ids)
import time
from Zoom.EventStreamer.Topic.topic import Topic
from Zoom.CarRentalSystem.Repository.booking_repository import BookingRepository
from Zoom.CarRentalSystem.metrics import (
    booking_events_processed_total,
    booking_slots_pending,
    booking_slots_booked,
    booking_lock_failures_total,
    event_processing_duration_seconds,
)


class BookingService:
    def __init__(self, repo: BookingRepository, event_streamer):
        self._repo = repo
        self._event_streamer = event_streamer

    async def book_vehicles(self, event) -> None:
        """Handles BookEvent: mark pending in DB → publish PaymentRequestEvent.
        DB transaction holds SELECT FOR UPDATE, so no in-process locks needed."""
        t0 = time.perf_counter()
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.correlation_id

        success = await self._repo.mark_pending(booking_id, vehicle_ids, from_date, to_date)
        if not success:
            booking_lock_failures_total.inc()
            print(f"[BookingService] Slots unavailable for booking={booking_id[:8]}")
            # Publish failure event so TicketService can respond to frontend
            from Zoom.EventStreamer.Event.event import BookingFailedEvent
            await self._event_streamer.add(
                Topic.TicketTopic,
                BookingFailedEvent(
                    correlation_id=booking_id,
                    booking_id=booking_id,
                    user_id=event.user_id,
                    vehicle_ids=vehicle_ids,
                    from_date=from_date,
                    to_date=to_date,
                    reason="Selected vehicles are not available for the requested dates",
                ),
            )
            booking_events_processed_total.labels(event_type="book_failed").inc()
            return

        booking_slots_pending.inc(len(vehicle_ids) * (to_date - from_date + 1))

        # DB committed — now publish (known gap: publish failure after commit is acceptable for now)
        from Zoom.EventStreamer.Event.event import PaymentRequestEvent
        await self._event_streamer.add(
            Topic.PaymentTopic,
            PaymentRequestEvent(
                correlation_id=booking_id,
                booking_id=booking_id,
                user_id=event.user_id,
                amount=100.0,  # TODO: calculate from vehicle type + date range
                vehicle_ids=vehicle_ids,
                from_date=from_date,
                to_date=to_date,
            ),
        )
        booking_events_processed_total.labels(event_type="book").inc()
        event_processing_duration_seconds.labels(event_type="book").observe(time.perf_counter() - t0)

    async def confirm_booking(self, event) -> None:
        """Handles PaymentSuccessEvent: mark booked in DB → publish GenerateTicketEvent."""
        t0 = time.perf_counter()
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.booking_id

        await self._repo.confirm_booking(booking_id, vehicle_ids, from_date, to_date)

        slots = len(vehicle_ids) * (to_date - from_date + 1)
        booking_slots_pending.dec(slots)
        booking_slots_booked.inc(slots)

        from Zoom.EventStreamer.Event.event import GenerateTicketEvent
        await self._event_streamer.add(
            Topic.TicketTopic,
            GenerateTicketEvent(
                correlation_id=booking_id,
                booking_id=booking_id,
                user_id=event.user_id,
                vehicle_ids=vehicle_ids,
                from_date=from_date,
                to_date=to_date,
            ),
        )
        booking_events_processed_total.labels(event_type="payment_success").inc()
        event_processing_duration_seconds.labels(event_type="payment_success").observe(time.perf_counter() - t0)

    async def remove_booking(self, event) -> None:
        """Handles PaymentFailureEvent: delete pending slots from DB."""
        t0 = time.perf_counter()
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.booking_id

        await self._repo.remove_booking(booking_id, vehicle_ids, from_date, to_date)

        booking_slots_pending.dec(len(vehicle_ids) * (to_date - from_date + 1))
        booking_events_processed_total.labels(event_type="payment_failure").inc()
        event_processing_duration_seconds.labels(event_type="payment_failure").observe(time.perf_counter() - t0)
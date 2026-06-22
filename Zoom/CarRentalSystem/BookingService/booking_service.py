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
    def __init__(self, repo: BookingRepository):
        self._repo = repo

    async def book_vehicles(self, event) -> None:
        """Handles BookEvent: mark pending in DB + write outbox event atomically."""
        t0 = time.perf_counter()
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.correlation_id

        from Zoom.EventStreamer.Event.event import PaymentRequestEvent, BookingFailedEvent

        success_event = PaymentRequestEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            amount=100.0,
            vehicle_ids=vehicle_ids,
            from_date=from_date,
            to_date=to_date,
        )
        failure_event = BookingFailedEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            vehicle_ids=vehicle_ids,
            from_date=from_date,
            to_date=to_date,
            reason="Selected vehicles are not available for the requested dates",
        )

        success = await self._repo.mark_pending(
            booking_id, vehicle_ids, from_date, to_date,
            success_outbox={"topic": Topic.PaymentTopic.value, "payload": success_event.model_dump(mode="json")},
            failure_outbox={"topic": Topic.TicketTopic.value,  "payload": failure_event.model_dump(mode="json")},
        )

        if not success:
            booking_lock_failures_total.inc()
            print(f"[BookingService] Slots unavailable for booking={booking_id[:8]}")
            booking_events_processed_total.labels(event_type="book_failed").inc()
            return

        booking_slots_pending.inc(len(vehicle_ids) * (to_date - from_date + 1))
        booking_events_processed_total.labels(event_type="book").inc()
        event_processing_duration_seconds.labels(event_type="book").observe(time.perf_counter() - t0)

    async def confirm_booking(self, event) -> None:
        """Handles PaymentSuccessEvent: mark booked in DB + write outbox event atomically."""
        t0 = time.perf_counter()
        vehicle_ids = event.vehicle_ids
        from_date = event.from_date
        to_date = event.to_date
        booking_id = event.booking_id

        from Zoom.EventStreamer.Event.event import GenerateTicketEvent

        outbox_event = GenerateTicketEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            vehicle_ids=vehicle_ids,
            from_date=from_date,
            to_date=to_date,
        )

        await self._repo.confirm_booking(
            booking_id, vehicle_ids, from_date, to_date,
            outbox={"topic": Topic.TicketTopic.value, "payload": outbox_event.model_dump(mode="json")},
        )

        slots = len(vehicle_ids) * (to_date - from_date + 1)
        booking_slots_pending.dec(slots)
        booking_slots_booked.inc(slots)
        booking_events_processed_total.labels(event_type="payment_success").inc()
        event_processing_duration_seconds.labels(event_type="payment_success").observe(time.perf_counter() - t0)

    async def remove_booking(self, event) -> None:
        """Handles PaymentFailureEvent: delete pending slots. Terminal — no outbox event."""
        t0 = time.perf_counter()
        await self._repo.remove_booking(
            event.booking_id, event.vehicle_ids, event.from_date, event.to_date,
        )
        booking_slots_pending.dec(len(event.vehicle_ids) * (event.to_date - event.from_date + 1))
        booking_events_processed_total.labels(event_type="payment_failure").inc()
        event_processing_duration_seconds.labels(event_type="payment_failure").observe(time.perf_counter() - t0)

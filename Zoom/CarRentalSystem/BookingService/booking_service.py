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
        t0 = time.perf_counter()
        booking_id = event.correlation_id
        vehicles   = [(v.vehicle_id, v.from_date, v.to_date) for v in event.vehicles]

        from Zoom.EventStreamer.Event.event import PaymentRequestEvent, BookingFailedEvent

        total_slots = sum(v.to_date - v.from_date + 1 for v in event.vehicles)
        amount      = total_slots * 100.0

        success_event = PaymentRequestEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            amount=amount,
            vehicles=event.vehicles,
        )
        failure_event = BookingFailedEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            vehicles=event.vehicles,
            reason="Selected vehicles are not available for the requested dates",
        )

        success = await self._repo.mark_pending(
            booking_id, vehicles,
            success_outbox={"topic": Topic.PaymentTopic.value, "payload": success_event.model_dump(mode="json")},
            failure_outbox={"topic": Topic.TicketTopic.value,  "payload": failure_event.model_dump(mode="json")},
        )

        if not success:
            booking_lock_failures_total.inc()
            print(f"[BookingService] Slots unavailable for booking={booking_id[:8]}")
            booking_events_processed_total.labels(event_type="book_failed").inc()
            return

        booking_slots_pending.inc(total_slots)
        booking_events_processed_total.labels(event_type="book").inc()
        event_processing_duration_seconds.labels(event_type="book").observe(time.perf_counter() - t0)

    async def confirm_booking(self, event) -> None:
        t0 = time.perf_counter()
        booking_id  = event.booking_id
        vehicle_ids = [v.vehicle_id for v in event.vehicles]
        total_slots = sum(v.to_date - v.from_date + 1 for v in event.vehicles)

        from Zoom.EventStreamer.Event.event import GenerateTicketEvent

        outbox_event = GenerateTicketEvent(
            correlation_id=booking_id,
            booking_id=booking_id,
            user_id=event.user_id,
            vehicles=event.vehicles,
        )

        await self._repo.confirm_booking(
            booking_id, vehicle_ids,
            outbox={"topic": Topic.TicketTopic.value, "payload": outbox_event.model_dump(mode="json")},
        )

        booking_slots_pending.dec(total_slots)
        booking_slots_booked.inc(total_slots)
        booking_events_processed_total.labels(event_type="payment_success").inc()
        event_processing_duration_seconds.labels(event_type="payment_success").observe(time.perf_counter() - t0)

    async def remove_booking(self, event) -> None:
        t0 = time.perf_counter()
        vehicle_ids = [v.vehicle_id for v in event.vehicles]
        total_slots = sum(v.to_date - v.from_date + 1 for v in event.vehicles)

        await self._repo.remove_booking(event.booking_id, vehicle_ids)

        booking_slots_pending.dec(total_slots)
        booking_events_processed_total.labels(event_type="payment_failure").inc()
        event_processing_duration_seconds.labels(event_type="payment_failure").observe(time.perf_counter() - t0)

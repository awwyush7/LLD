import time
from Zoom.EventStreamer.Topic.topic import Topic
# from Zoom.CarRentalSystem.metrics import payments_processed_total, payment_duration_seconds


class PaymentService:
    def __init__(self, event_streamer):
        self._event_streamer = event_streamer

    async def process_payment(self, event) -> None:
        """Handles PaymentRequestEvent: charge → publish PaymentSuccessEvent or PaymentFailureEvent to BookingTopic."""
        t0 = time.perf_counter()
        success = await self._charge(event.user_id, event.amount)

        from Zoom.EventStreamer.Event.event import PaymentSuccessEvent, PaymentFailureEvent

        if success:
            result = PaymentSuccessEvent(
                correlation_id=event.correlation_id,
                booking_id=event.booking_id,
                user_id=event.user_id,
                vehicle_ids=event.vehicle_ids,
                from_date=event.from_date,
                to_date=event.to_date,
            )
        else:
            result = PaymentFailureEvent(
                correlation_id=event.correlation_id,
                booking_id=event.booking_id,
                user_id=event.user_id,
                vehicle_ids=event.vehicle_ids,
                from_date=event.from_date,
                to_date=event.to_date,
                reason="Payment declined",
            )

        await self._event_streamer.add(Topic.BookingTopic, result)
        # payments_processed_total.labels(result="success" if success else "failure").inc()
        # payment_duration_seconds.observe(time.perf_counter() - t0)

    async def _charge(self, user_id: str, amount: float) -> bool:
        # Stub: replace with Stripe / Razorpay call
        return True
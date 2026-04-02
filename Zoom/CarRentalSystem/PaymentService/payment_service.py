from Zoom.EventStreamer.Topic.topic import Topic


class PaymentService:
    def __init__(self, event_streamer):
        self._event_streamer = event_streamer

    async def process_payment(self, event) -> None:
        """Handles PaymentREquestedEvent: charge -> publuc PaymentSuccessEvent or PaymentFailureEvent to BookingTopic"""
        success = await self._charge(event.user_id, event.amount)

        from Zoom.EventStreamer.Event.event import PaymentFailureEvent, PaymentSuccessEvent

        if success:
            result = PaymentSuccessEvent(
                correlation_id = event.correlation_id,
                booking_id = event.booking_id,
                user_id = event.user_id,
                vehicle_ids = event.vehicle_ids,
                from_date = event.from_date,
                to_date = event.to_date,
            )
        else:
            result = PaymentFailureEvent(
                correlation_id = event.correlation_id,
                booking_id = event.booking_id,
                user_id = event.user_id,
                vehicle_ids = event.vehicle_ids,
                from_date = event.from_date,
                to_date = event.to_date,
                reason = "Payment Declined",
            )

        await self._event_streamer.add(Topic.BookingTopic, result)

    async def _charge(self, user_id : str, amount: float) -> bool:
        return True
    
# python3 -m Zoom.CarRentalSystem.PaymentService.main
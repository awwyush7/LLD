import httpx
from Zoom.CarRentalSystem.Repository.payment_intent_repository import PaymentIntentRepository


class PaymentService:
    def __init__(self, repo: PaymentIntentRepository, stripe_url: str):
        self._repo = repo
        self._stripe_url = stripe_url
        self._http = httpx.AsyncClient()

    async def process_payment(self, event) -> None:
        """
        Handles PaymentRequestEvent.
        1. Record intent in DB (idempotent).
        2. Call Stripe to create a checkout session → get redirect_url.
        3. Store session info. Done — no event published here.
        Stripe will call our webhook when the user pays (or fails).
        """
        await self._repo.create(
            booking_id=event.booking_id,
            user_id=event.user_id,
            amount=event.amount,
            vehicle_ids=event.vehicle_ids,
            from_date=event.from_date,
            to_date=event.to_date,
        )

        resp = await self._http.post(
            f"{self._stripe_url}/v1/checkout/sessions",
            json={
                "booking_id": event.booking_id,
                "amount": event.amount,
                "user_id": event.user_id,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        await self._repo.mark_awaiting_payment(
            booking_id=event.booking_id,
            stripe_session_id=data["session_id"],
            redirect_url=data["redirect_url"],
        )
        print(f"[PaymentService] Intent awaiting payment  booking={event.booking_id[:8]}")

    async def close(self) -> None:
        await self._http.aclose()

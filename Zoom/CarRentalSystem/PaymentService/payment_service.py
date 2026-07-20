import time
import httpx
from redis.asyncio import Redis
from Zoom.CarRentalSystem.Repository.payment_intent_repository import PaymentIntentRepository
from Zoom.CarRentalSystem.metrics import payments_processed_total, payment_duration_seconds

_INTENT_TTL = 86_400  # 24 hours — covers any realistic Kafka retry window


class PaymentService:
    def __init__(self, repo: PaymentIntentRepository, stripe_url: str, redis: Redis):
        self._repo = repo
        self._stripe_url = stripe_url
        self._http = httpx.AsyncClient()
        self._redis = redis

    async def process_payment(self, event) -> None:
        """
        Handles PaymentRequestEvent.
        Fast idempotency check via Redis before touching Postgres or Stripe.
        Postgres ON CONFLICT DO NOTHING remains the correctness backstop.
        """
        redis_key = f"intent:{event.booking_id}"
        t0 = time.perf_counter()

        if await self._redis.exists(redis_key):
            print(f"[PaymentService] Duplicate event skipped (Redis hit)  booking={event.booking_id[:8]}")
            return

        await self._repo.create(
            booking_id=event.booking_id,
            user_id=event.user_id,
            amount=event.amount,
            vehicles=[v.model_dump() for v in event.vehicles],
        )

        try:
            resp = await self._http.post(
                f"{self._stripe_url}/v1/checkout/sessions",
                json={
                    "booking_id": event.booking_id,
                    "amount": event.amount,
                    "user_id": event.user_id,
                },
            )
            resp.raise_for_status()
        except Exception:
            payments_processed_total.labels(result="error").inc()
            payment_duration_seconds.observe(time.perf_counter() - t0)
            raise
        data = resp.json()

        await self._repo.mark_awaiting_payment(
            booking_id=event.booking_id,
            stripe_session_id=data["session_id"],
            redirect_url=data["redirect_url"],
        )

        # Mark processed in Redis — future redeliveries short-circuit here
        await self._redis.set(redis_key, "1", ex=_INTENT_TTL)
        payments_processed_total.labels(result="initiated").inc()
        payment_duration_seconds.observe(time.perf_counter() - t0)
        print(f"[PaymentService] Intent awaiting payment  booking={event.booking_id[:8]}")

    async def close(self) -> None:
        await self._http.aclose()
        await self._redis.aclose()

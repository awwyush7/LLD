import json
import asyncpg
from typing import List, Optional
from Zoom.CarRentalSystem.Repository.payment_intent_repository import PaymentIntentRepository


class PostgresPaymentIntentRepository(PaymentIntentRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def create(
        self,
        booking_id: str,
        user_id: str,
        amount: float,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Idempotent — ON CONFLICT DO NOTHING guards against event redelivery."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO payment_intents
                    (booking_id, user_id, amount, vehicle_ids, from_date, to_date, status)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6, 'pending')
                ON CONFLICT (booking_id) DO NOTHING
                """,
                booking_id, user_id, amount, json.dumps(vehicle_ids), from_date, to_date,
            )

    async def mark_awaiting_payment(
        self,
        booking_id: str,
        stripe_session_id: str,
        redirect_url: str,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE payment_intents
                SET status = 'awaiting_payment',
                    stripe_session_id = $2,
                    redirect_url = $3,
                    updated_at = now()
                WHERE booking_id = $1
                """,
                booking_id, stripe_session_id, redirect_url,
            )

    async def mark_paid(self, booking_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE payment_intents
                SET status = 'paid', updated_at = now()
                WHERE booking_id = $1
                """,
                booking_id,
            )

    async def mark_failed(self, booking_id: str, reason: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE payment_intents
                SET status = 'failed', failure_reason = $2, updated_at = now()
                WHERE booking_id = $1
                """,
                booking_id, reason,
            )

    async def get(self, booking_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT booking_id, user_id, amount, vehicle_ids, from_date, to_date,
                       status, stripe_session_id, redirect_url, failure_reason
                FROM payment_intents WHERE booking_id = $1
                """,
                booking_id,
            )
        if row is None:
            return None
        result = dict(row)
        result["vehicle_ids"] = json.loads(result["vehicle_ids"])
        return result

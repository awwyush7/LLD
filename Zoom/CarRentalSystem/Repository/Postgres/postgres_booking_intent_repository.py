import json
import asyncpg
from typing import List, Optional


class PostgresBookingIntentRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def create(
        self,
        booking_id: str,
        user_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """
        Idempotent. If the same booking_id arrives twice (client retried POST
        /booking-intent before receiving the response), ON CONFLICT DO NOTHING
        silently ignores the duplicate — the caller gets the same booking_id back.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO booking_intents (booking_id, user_id, vehicle_ids, from_date, to_date)
                VALUES ($1, $2, $3::jsonb, $4, $5)
                ON CONFLICT (booking_id) DO NOTHING
                """,
                booking_id, user_id, json.dumps(vehicle_ids), from_date, to_date,
            )

    async def get(self, booking_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT booking_id, user_id, vehicle_ids, from_date, to_date, confirmed
                FROM booking_intents WHERE booking_id = $1
                """,
                booking_id,
            )
        if row is None:
            return None
        return {
            "booking_id": row["booking_id"],
            "user_id":     row["user_id"],
            "vehicle_ids": json.loads(row["vehicle_ids"]),
            "from_date":   row["from_date"],
            "to_date":     row["to_date"],
            "confirmed":   row["confirmed"],
        }

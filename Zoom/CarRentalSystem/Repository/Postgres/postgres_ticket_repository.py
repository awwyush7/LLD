import json
import asyncpg
from typing import List, Optional
from Zoom.CarRentalSystem.Repository.ticket_repository import TicketRepository


class PostgresTicketRepository(TicketRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def save(
        self,
        booking_id: str,
        user_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
        status: str = "confirmed",
        reason: Optional[str] = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tickets (booking_id, user_id, vehicle_ids, from_date, to_date, status)
                VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                ON CONFLICT (booking_id) DO NOTHING
                """,
                booking_id, user_id, json.dumps(vehicle_ids), from_date, to_date, status,
            )

    async def get(self, booking_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT booking_id, user_id, vehicle_ids, from_date, to_date, status, reason "
                "FROM tickets WHERE booking_id = $1",
                booking_id,
            )
        if row is None:
            return None
        result = {
            "booking_id":  row["booking_id"],
            "user_id":     row["user_id"],
            "vehicle_ids": json.loads(row["vehicle_ids"]),
            "from_date":   row["from_date"],
            "to_date":     row["to_date"],
            "status":      row["status"],
        }
        if row["reason"]:
            result["reason"] = row["reason"]
        return result
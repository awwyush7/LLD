import json
from typing import List
import asyncpg
from Zoom.CarRentalSystem.Repository.booking_repository import BookingRepository


class PostgresBookingRepository(BookingRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def mark_pending(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
        success_outbox: dict,
        failure_outbox: dict,
    ) -> bool:
        sorted_ids = sorted(vehicle_ids)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )

                conflict_count = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM bookings
                    WHERE vehicle_id = ANY($1::text[])
                      AND date BETWEEN $2 AND $3
                      AND status IN ('pending', 'booked')
                    """,
                    sorted_ids, from_date, to_date,
                )

                if conflict_count > 0:
                    # Slots unavailable — write failure event to outbox atomically
                    await conn.execute(
                        "INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)",
                        failure_outbox["topic"], json.dumps(failure_outbox["payload"]),
                    )
                    return False

                rows = [
                    (booking_id, vid, day, "pending")
                    for vid in sorted_ids
                    for day in range(from_date, to_date + 1)
                ]
                await conn.executemany(
                    "INSERT INTO bookings (booking_id, vehicle_id, date, status) VALUES ($1, $2, $3, $4)",
                    rows,
                )
                # Write success event to outbox in the same transaction
                await conn.execute(
                    "INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)",
                    success_outbox["topic"], json.dumps(success_outbox["payload"]),
                )
        return True

    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
        outbox: dict,
    ) -> None:
        sorted_ids = sorted(vehicle_ids)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )
                await conn.execute(
                    "UPDATE bookings SET status = 'booked' WHERE booking_id = $1",
                    booking_id,
                )
                await conn.execute(
                    "INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)",
                    outbox["topic"], json.dumps(outbox["payload"]),
                )

    async def remove_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        sorted_ids = sorted(vehicle_ids)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )
                await conn.execute(
                    "DELETE FROM bookings WHERE booking_id = $1",
                    booking_id,
                )

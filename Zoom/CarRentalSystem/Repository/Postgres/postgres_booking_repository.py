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
    ) -> bool:
        """
        Atomically:
          1. Lock vehicle rows in sorted order (prevents deadlocks across concurrent requests)
          2. Check for any conflicting pending/booked slots
          3. If clean, insert one row per (vehicle, date) as 'pending'
        Returns True on success, False if any slot is already taken.
        """
        sorted_ids = sorted(vehicle_ids)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: acquire row-level locks on vehicles, sorted to prevent deadlocks
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )

                # Step 2: conflict check
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
                    return False

                # Step 3: insert pending rows (one per vehicle per date)
                rows = [
                    (booking_id, vid, day, "pending")
                    for vid in sorted_ids
                    for day in range(from_date, to_date + 1)
                ]
                await conn.executemany(
                    "INSERT INTO bookings (booking_id, vehicle_id, date, status) "
                    "VALUES ($1, $2, $3, $4)",
                    rows,
                )
        return True

    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Transitions pending → booked for all slots belonging to booking_id."""
        sorted_ids = sorted(vehicle_ids)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Lock the same vehicle rows before mutating bookings
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )
                await conn.execute(
                    """
                    UPDATE bookings SET status = 'booked'
                    WHERE booking_id = $1
                    """,
                    booking_id,
                )

    async def remove_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Removes all pending slots for booking_id (payment failure / cancel)."""
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
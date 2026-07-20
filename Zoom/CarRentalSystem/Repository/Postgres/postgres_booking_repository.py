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
        vehicles: List[tuple],   # List of (vehicle_id, from_date, to_date)
        success_outbox: dict,
        failure_outbox: dict,
    ) -> bool:
        sorted_vehicles = sorted(vehicles, key=lambda v: v[0])   # sort by vehicle_id
        sorted_ids  = [v[0] for v in sorted_vehicles]
        from_dates  = [v[1] for v in sorted_vehicles]
        to_dates    = [v[2] for v in sorted_vehicles]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Lock vehicle rows in sorted order — prevents deadlocks
                await conn.fetch(
                    "SELECT id FROM vehicles WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE",
                    sorted_ids,
                )

                # Deduplication: relay may redeliver the same BookEvent
                already_processed = await conn.fetchval(
                    "SELECT COUNT(*) FROM bookings WHERE booking_id = $1",
                    booking_id,
                )
                if already_processed > 0:
                    return True

                # Per-vehicle conflict check: each vehicle has its own date range
                conflict_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM bookings b
                    JOIN UNNEST($1::text[], $2::int[], $3::int[])
                         AS req(vehicle_id, from_date, to_date)
                      ON b.vehicle_id = req.vehicle_id
                     AND b.date BETWEEN req.from_date AND req.to_date
                     AND b.status IN ('pending', 'booked')
                    """,
                    sorted_ids, from_dates, to_dates,
                )

                if conflict_count > 0:
                    await conn.execute(
                        "INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)",
                        failure_outbox["topic"], json.dumps(failure_outbox["payload"]),
                    )
                    return False

                # One row per (vehicle, day) using each vehicle's own date range
                rows = [
                    (booking_id, vid, day, "pending")
                    for (vid, from_d, to_d) in sorted_vehicles
                    for day in range(from_d, to_d + 1)
                ]
                await conn.executemany(
                    "INSERT INTO bookings (booking_id, vehicle_id, date, status) VALUES ($1, $2, $3, $4)",
                    rows,
                )
                await conn.execute(
                    "INSERT INTO outbox (topic, payload) VALUES ($1, $2::jsonb)",
                    success_outbox["topic"], json.dumps(success_outbox["payload"]),
                )
        return True

    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
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

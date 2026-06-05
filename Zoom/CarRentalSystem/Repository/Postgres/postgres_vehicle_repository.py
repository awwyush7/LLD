import asyncpg
from Zoom.CarRentalSystem.Repository.vehicle_repository import VehicleRepository


class PostgresVehicleRepository(VehicleRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def add(
        self,
        id: str,
        owner_id: str,
        plate_no: str,
        model_year: int,
        status: str = "available",
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO vehicles (id, owner_id, plate_no, model_year, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING
                """,
                id, owner_id, plate_no, model_year, status,
            )

    async def exists(self, id: str) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM vehicles WHERE id = $1", id
            )
            return row is not None
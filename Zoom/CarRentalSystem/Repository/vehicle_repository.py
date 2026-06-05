from abc import ABC, abstractmethod


class VehicleRepository(ABC):
    @abstractmethod
    async def add(
        self,
        id: str,
        owner_id: str,
        plate_no: str,
        model_year: int,
        status: str = "available",
    ) -> None:
        """Insert a vehicle. Silently ignores duplicate IDs (idempotent seed)."""

    @abstractmethod
    async def exists(self, id: str) -> bool:
        """Returns True if a vehicle with the given id is present."""
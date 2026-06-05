from abc import ABC, abstractmethod
from typing import List


class BookingRepository(ABC):
    @abstractmethod
    async def mark_pending(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> bool:
        """Atomically checks availability and marks slots as pending.
        Returns True if successful, False if any slot is already taken."""

    @abstractmethod
    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Transitions all pending slots for booking_id to 'booked'."""

    @abstractmethod
    async def remove_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Removes all pending slots for booking_id (payment failure / cancel)."""
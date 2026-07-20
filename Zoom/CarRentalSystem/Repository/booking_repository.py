from abc import ABC, abstractmethod
from typing import List


class BookingRepository(ABC):
    @abstractmethod
    async def mark_pending(
        self,
        booking_id: str,
        vehicles: List[tuple],   # List of (vehicle_id, from_date, to_date)
        success_outbox: dict,
        failure_outbox: dict,
    ) -> bool:
        """Atomically checks per-vehicle availability, marks slots pending, writes outbox event.
        Returns True if successful, False if any slot is already taken."""

    @abstractmethod
    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],  # IDs only needed for row-level locking
        outbox: dict,
    ) -> None:
        """Transitions all pending slots to booked and writes one outbox event."""

    @abstractmethod
    async def remove_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],  # IDs only needed for row-level locking
    ) -> None:
        """Removes all pending slots for booking_id. Terminal — no outbox event."""

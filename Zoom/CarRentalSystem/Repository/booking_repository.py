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
        success_outbox: dict,   # {topic, payload} written to outbox on success
        failure_outbox: dict,   # {topic, payload} written to outbox on conflict
    ) -> bool:
        """Atomically checks availability, marks slots pending, and writes one outbox event.
        Returns True if successful, False if any slot is already taken."""

    @abstractmethod
    async def confirm_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
        outbox: dict,           # {topic, payload} written to outbox atomically
    ) -> None:
        """Transitions all pending slots to booked and writes one outbox event."""

    @abstractmethod
    async def remove_booking(
        self,
        booking_id: str,
        vehicle_ids: List[str],
        from_date: int,
        to_date: int,
    ) -> None:
        """Removes all pending slots for booking_id. Terminal action — no outbox event."""

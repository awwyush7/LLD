from abc import ABC, abstractmethod
from typing import List, Optional


class TicketRepository(ABC):
    @abstractmethod
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
        """Persist a ticket (confirmed or failed). Idempotent on duplicate booking_id."""

    @abstractmethod
    async def get(self, booking_id: str) -> Optional[dict]:
        """Return the ticket dict or None if not found."""
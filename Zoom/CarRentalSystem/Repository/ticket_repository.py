from abc import ABC, abstractmethod
from typing import List, Optional


class TicketRepository(ABC):
    @abstractmethod
    async def save(
        self,
        booking_id: str,
        user_id: str,
        vehicles: List[dict],    # [{vehicle_id, from_date, to_date}, ...]
        status: str = "confirmed",
        reason: Optional[str] = None,
    ) -> None:
        """Persist a ticket. Idempotent on duplicate booking_id."""

    @abstractmethod
    async def get(self, booking_id: str) -> Optional[dict]:
        """Return the ticket dict or None if not found."""

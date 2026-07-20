from abc import ABC, abstractmethod
from typing import List, Optional


class PaymentIntentRepository(ABC):
    @abstractmethod
    async def create(
        self,
        booking_id: str,
        user_id: str,
        amount: float,
        vehicles: List[dict],    # List of {vehicle_id, from_date, to_date}
    ) -> None:
        """Insert a new intent row with status 'pending'. No-op if booking_id already exists."""

    @abstractmethod
    async def mark_awaiting_payment(
        self,
        booking_id: str,
        stripe_session_id: str,
        redirect_url: str,
    ) -> None:
        """Transition pending → awaiting_payment, store Stripe session info."""

    @abstractmethod
    async def mark_paid(self, booking_id: str) -> None:
        """Transition awaiting_payment → paid."""

    @abstractmethod
    async def mark_failed(self, booking_id: str, reason: str) -> None:
        """Transition awaiting_payment → failed."""

    @abstractmethod
    async def get(self, booking_id: str) -> Optional[dict]:
        """Return the intent dict or None if not found."""

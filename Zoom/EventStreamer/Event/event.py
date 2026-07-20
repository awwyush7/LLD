from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal, Union, Annotated, List
import uuid
from datetime import datetime


class VehicleBooking(BaseModel):
    vehicle_id: str
    from_date: int
    to_date: int


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class BookEvent(Event):
    type: Literal["book"] = "book"
    user_id: str
    vehicles: List[VehicleBooking]


class PaymentRequestEvent(Event):
    type: Literal["payment_request"] = "payment_request"
    booking_id: str
    user_id: str
    amount: float
    vehicles: List[VehicleBooking]


class PaymentSuccessEvent(Event):
    type: Literal["payment_success"] = "payment_success"
    booking_id: str
    user_id: str
    vehicles: List[VehicleBooking]


class PaymentFailureEvent(Event):
    type: Literal["payment_failure"] = "payment_failure"
    booking_id: str
    user_id: str
    vehicles: List[VehicleBooking]
    reason: str


class GenerateTicketEvent(Event):
    type: Literal["generate_ticket"] = "generate_ticket"
    booking_id: str
    user_id: str
    vehicles: List[VehicleBooking]


class BookingFailedEvent(Event):
    type: Literal["booking_failed"] = "booking_failed"
    booking_id: str
    user_id: str
    vehicles: List[VehicleBooking]
    reason: str


AnyEvent = Annotated[
    Union[
        BookEvent,
        PaymentRequestEvent,
        PaymentSuccessEvent,
        PaymentFailureEvent,
        GenerateTicketEvent,
        BookingFailedEvent,
    ],
    Field(discriminator="type"),
]

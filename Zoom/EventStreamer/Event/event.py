from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal, Union, Annotated, List 
import uuid
from datetime import datetime


class Event (BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class BookEvent (Event):
    type: Literal["book"] = "book"
    vehicle_ids: List[str]
    user_id: str 
    from_date: int 
    to_date: int


class PaymentRequestEvent(Event):
    type: Literal["payment_request"] = "payment_request"
    booking_id: str 
    user_id:str 
    amount: float 
    vehicle_ids: List[str]
    from_date: int 
    to_date: int


class PaymentSuccessEvent (Event) :
    type: Literal["payment_success"] = "payment_success"
    booking_id: str 
    user_id: str
    vehicle_ids: List[str]
    from_date: int 
    to_date: int


class PaymentFailureEvent(Event):
    type : Literal["payment_failure"] = "payment_failure"
    booking_id: str
    user_id: str
    vehicle_ids: List[str]
    from_date: int
    to_date: int
    reason: str


class GenerateTicketEvent(Event):
    type : Literal["generate_ticket"] = "generate_ticket"
    booking_id: str
    user_id: str
    vehicle_ids: List[str]
    from_date: int
    to_date: int

class BookingFailedEvent(Event):
    type: Literal["booking_failed"] = "booking_failed"
    booking_id: str
    user_id: str
    vehicle_ids: List[str]
    from_date: int
    to_date: int
    reason:str

AnyEvent = Annotated[
    Union[
        BookEvent,
        PaymentRequestEvent,
        PaymentSuccessEvent,
        PaymentFailureEvent,
        GenerateTicketEvent,
        BookingFailedEvent
    ],
    Field(discriminator="type")
]
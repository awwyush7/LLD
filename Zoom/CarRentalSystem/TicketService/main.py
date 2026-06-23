
"""
TicketService — pure consumer + read API (port 8001)

Responsibilities after the split:
  • Consumes TicketTopic from Kafka → writes confirmed/failed rows to tickets table.
  • GET /ticket/{id}           — browser polls for booking outcome.
  • GET /payment/status/{id}   — browser polls for payment redirect URL.
  • POST /webhook/payment      — called by Stripe/MockStripe after payment.

Booking intake has moved to IngestionService (port 8003):
  POST /booking-intent  →  create intent in Postgres
  POST /confirm/{id}    →  write to outbox → OutboxRelay → Kafka

Run:
    uvicorn Zoom.CarRentalSystem.TicketService.main:app --port 8001
"""
from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, TypeAdapter
from typing import List

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.Repository.Postgres.postgres_ticket_repository import PostgresTicketRepository
from Zoom.CarRentalSystem.Repository.Postgres.postgres_payment_intent_repository import PostgresPaymentIntentRepository
from Zoom.EventStreamer.Event.event import AnyEvent, PaymentSuccessEvent, PaymentFailureEvent
from Zoom.EventStreamer.Topic.topic import Topic
from Zoom.CarRentalSystem.metrics import (
    metrics_endpoint,
    tickets_confirmed_total,
    http_request_duration_seconds,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ticket-service-group")

event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
adapter = TypeAdapter(AnyEvent)
ticket_repo: PostgresTicketRepository | None = None
intent_repo: PostgresPaymentIntentRepository | None = None


# ---------------------------------------------------------------------------
# Background consumer — listens on TicketTopic, writes confirmed tickets to DB
# ---------------------------------------------------------------------------
async def _handle_ticket_event(raw: dict) -> None:
    """Handler passed to process_with_dlq — processes one TicketTopic message."""
    event = adapter.validate_python(raw)
    if event.type == "generate_ticket":
        print(f"[TicketService] Ticket confirmed  booking={event.booking_id[:8]}")
        await ticket_repo.save(
            booking_id=event.booking_id,
            user_id=event.user_id,
            vehicle_ids=event.vehicle_ids,
            from_date=event.from_date,
            to_date=event.to_date,
            status="confirmed",
        )
        tickets_confirmed_total.inc()
    elif event.type == "booking_failed":
        print(f"[TicketService] Booking failed  booking={event.booking_id[:8]} reason={event.reason}")
        await ticket_repo.save(
            booking_id=event.booking_id,
            user_id=event.user_id,
            vehicle_ids=event.vehicle_ids,
            from_date=event.from_date,
            to_date=event.to_date,
            status="failed",
            reason=event.reason,
        )


async def ticket_consumer():
    print("[TicketService] Listening on TicketTopic...")
    while True:
        # Retry up to 3 times with exponential backoff.
        # On permanent failure, message goes to TicketTopicDLQ so the partition
        # is never blocked. DB is idempotent (ON CONFLICT DO NOTHING), so
        # redeliveries after a transient failure are absorbed safely.
        await event_handler.process_with_dlq(
            Topic.TicketTopic.value,
            Topic.TicketTopicDLQ.value,
            _handle_ticket_event,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ticket_repo, intent_repo
    await event_handler.start()
    print(f"[TicketService] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    pool = await get_pool()
    ticket_repo = PostgresTicketRepository(pool)
    intent_repo = PostgresPaymentIntentRepository(pool)
    task = asyncio.create_task(ticket_consumer())
    yield
    task.cancel()
    await event_handler.stop()
    await close_pool()


app = FastAPI(title="TicketService / Gateway", lifespan=lifespan)

# Allow the standalone Frontend to call this service from file:// or any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def track_latency(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    if request.url.path != "/metrics":
        http_request_duration_seconds.labels(
            method=request.method, path=request.url.path
        ).observe(time.perf_counter() - start)
    return response


app.add_route("/metrics", metrics_endpoint)



# /book has moved to IngestionService (POST /booking-intent + POST /confirm/{id})

@app.get("/ticket/{booking_id}")
async def get_ticket(booking_id: str):
    ticket = await ticket_repo.get(booking_id)
    return ticket if ticket is not None else {"status": "pending"}


@app.get("/payment/status/{booking_id}")
async def get_payment_status(booking_id: str):
    """Frontend polls this to get redirect_url and track payment progress."""
    intent = await intent_repo.get(booking_id)
    if intent is None:
        return {"status": "pending"}
    return {
        "status": intent["status"],
        "redirect_url": intent.get("redirect_url"),
        "failure_reason": intent.get("failure_reason"),
    }


class WebhookPayload(BaseModel):
    session_id: str
    booking_id: str
    status: str
    reason: str | None = None


@app.post("/webhook/payment")
async def payment_webhook(payload: WebhookPayload):
    """
    Called by Stripe (MockStripe in dev) after the user completes or fails payment.
    Updates the intent status and fires the appropriate event into BookingTopic.
    """
    intent = await intent_repo.get(payload.booking_id)
    if intent is None:
        return {"error": "unknown booking_id"}

    if payload.status == "paid":
        await intent_repo.mark_paid(payload.booking_id)
        await event_handler.add(
            Topic.BookingTopic,
            PaymentSuccessEvent(
                correlation_id=payload.booking_id,
                booking_id=payload.booking_id,
                user_id=intent["user_id"],
                vehicle_ids=intent["vehicle_ids"],
                from_date=intent["from_date"],
                to_date=intent["to_date"],
            ),
        )
        print(f"[TicketService] Webhook: payment paid  booking={payload.booking_id[:8]}")
    else:
        reason = payload.reason or "Payment failed"
        await intent_repo.mark_failed(payload.booking_id, reason)
        await event_handler.add(
            Topic.BookingTopic,
            PaymentFailureEvent(
                correlation_id=payload.booking_id,
                booking_id=payload.booking_id,
                user_id=intent["user_id"],
                vehicle_ids=intent["vehicle_ids"],
                from_date=intent["from_date"],
                to_date=intent["to_date"],
                reason=reason,
            ),
        )
        print(f"[TicketService] Webhook: payment failed  booking={payload.booking_id[:8]}")

    return {"received": True}

"""
TicketService — API Gateway + Ticket Consumer
Runs on port 8001. Accepts bookings, polls for tickets.
UI is now served separately from Zoom/CarRentalSystem/Frontend/index.html.

Run from C:\\Learning\\lld_new\\LLD:
    $env:PYTHONPATH = "C:\\Learning\\lld_new\\LLD"
    uvicorn Zoom.CarRentalSystem.TicketService.main:app --port 8001

Requires EventStreamer running on port 8000, BookingService and
PaymentService workers running separately.
"""
from dotenv import load_dotenv
load_dotenv()  # Load .env file before other imports

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, TypeAdapter

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.Repository.Postgres.postgres_ticket_repository import PostgresTicketRepository
from Zoom.CarRentalSystem.Repository.Postgres.postgres_payment_intent_repository import PostgresPaymentIntentRepository
from Zoom.EventStreamer.Event.event import BookEvent, AnyEvent, PaymentSuccessEvent, PaymentFailureEvent
from Zoom.EventStreamer.Topic.topic import Topic
from Zoom.CarRentalSystem.metrics import (
    metrics_endpoint,
    booking_requests_total,
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
async def ticket_consumer():
    # Kafka offset commit strategy used here: "at-least-once delivery"
    #
    # Timeline of a single message:
    #   1. getone()       → Kafka hands us the message; offset NOT advanced yet
    #   2. DB save        → side-effect committed to Postgres
    #   3. commit_offset  → Kafka advances the offset; message considered "done"
    #
    # If we crash between step 1 and step 3, Kafka redelivers the message on
    # reconnect. The DB INSERT … ON CONFLICT DO NOTHING absorbs the duplicate
    # write safely — this is what makes at-least-once tolerable in practice.
    #
    # The alternative is "exactly-once" via Kafka transactions, but that
    # requires the DB write and offset commit to be in a single atomic
    # transaction — only possible with the Kafka Streams API or a transactional
    # producer, not straightforward with asyncpg + aiokafka.
    print("[TicketService] Listening on TicketTopic...")
    while True:
        try:
            raw = await event_handler.get_tasks(Topic.TicketTopic.value)
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

            # Only reach here if the DB write succeeded.
            # Committing now tells Kafka: "even if I crash and restart,
            # don't redeliver this message — I already handled it."
            await event_handler.commit_offset(Topic.TicketTopic.value)

        except Exception as exc:
            # We deliberately do NOT commit on error.
            # Kafka will redeliver this message on the next getone() call,
            # giving us a chance to retry the DB write.
            print(f"[TicketService] consumer error: {exc}")


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


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
class BookRequest(BaseModel):
    vehicle_ids: List[str]
    user_id: str
    from_date: int
    to_date: int
    # Idempotency key: client generates a UUID once per logical booking attempt
    # and resends the same key on retries.
    #
    # Why this matters:
    #   Without it, each retry generates a fresh UUID → Kafka receives two
    #   BookEvents for the same seat → two locks acquired → two charges.
    #   With it, we detect "seen before" and short-circuit before touching Kafka.
    #
    # Pattern: client generates key, server uses it as booking_id. On retry
    # the DB check returns the existing record instead of re-publishing.
    idempotency_key: str | None = None


@app.post("/book")
async def book(req: BookRequest):
    booking_id = req.idempotency_key or str(uuid.uuid4())

    # Fast-path: if this booking_id is already in the DB, someone already
    # processed it (either this request succeeded before or a retry is coming in
    # after we published to Kafka but before the HTTP response reached the client).
    # Return the stored status so the client can continue polling /ticket/{id}.
    existing = await ticket_repo.get(booking_id)
    if existing is not None:
        return {"booking_id": booking_id, "status": existing["status"]}

    # send_and_wait + acks="all" + enable_idempotence means:
    #   • we block until every in-sync Kafka replica has written the message
    #   • the broker deduplicates any producer-level retries automatically
    # If we crash between this line and the return below, the event IS in Kafka.
    # The next retry with the same idempotency_key will hit the existing-check
    # above once the booking flows through and lands in the DB.
    await event_handler.add(
        Topic.BookingTopic,
        BookEvent(
            correlation_id=booking_id,
            vehicle_ids=req.vehicle_ids,
            user_id=req.user_id,
            from_date=req.from_date,
            to_date=req.to_date,
        ),
    )
    booking_requests_total.inc()
    return {"booking_id": booking_id, "status": "processing"}


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
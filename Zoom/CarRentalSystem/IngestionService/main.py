"""
IngestionService — stateless HTTP entry point for booking requests (port 8003).

Two-step flow (client carries the state between steps):

  Step 1  POST /booking-intent
          Pure UUID factory. No DB, no Kafka. Just returns a booking_id.
          Client stores this ID and uses it for step 2.
          If the response never arrives → client retries → gets a new UUID → fine,
          because no state was written anywhere. The old UUID is simply never used.

  Step 2  POST /confirm/{booking_id}
          Client sends the full booking payload. One DB write: INSERT INTO outbox
          with idempotency_key = booking_id and ON CONFLICT DO NOTHING.
          • First call   → row inserted → OutboxRelay picks it up → Kafka → done.
          • Retry / dup  → ON CONFLICT → row already there → silently ignored → 202.
          Crash before INSERT commits → transaction rolls back → retry inserts again.
          Crash after INSERT commits  → retry hits ON CONFLICT → idempotent.

  OutboxRelay (background task)
          Polls outbox for unpublished rows → publishes to Kafka → marks published.

Run:
    uvicorn Zoom.CarRentalSystem.IngestionService.main:app --port 8003
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.CarRentalSystem.BookingService.outbox_relay import OutboxRelay
from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.EventStreamer.Event.event import BookEvent
from Zoom.EventStreamer.Topic.topic import Topic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "ingestion-service-group")

_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await get_pool()

    event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    await event_handler.start()
    print(f"[IngestionService] Kafka producer connected at {KAFKA_BOOTSTRAP_SERVERS}")

    relay_task = asyncio.create_task(OutboxRelay(_pool, event_handler).run())
    yield
    relay_task.cancel()
    await event_handler.stop()
    await close_pool()


app = FastAPI(title="IngestionService", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------------
# Step 1 — stateless UUID factory
# ---------------------------------------------------------------------------
@app.post("/booking-intent", status_code=200)
async def create_booking_intent():
    """
    Returns a fresh booking_id. No DB write, no Kafka.

    Why no DB here:
    If we wrote to DB and the response never reached the client, the client
    would retry and create another row — orphaned garbage. Since there is no
    state to write at this point (we don't know if the user will confirm),
    the correct thing is to write nothing and let the client carry the ID.
    """
    return {"booking_id": str(uuid.uuid4())}


# ---------------------------------------------------------------------------
# Step 2 — durable confirm
# ---------------------------------------------------------------------------
class ConfirmRequest(BaseModel):
    user_id:     str
    vehicle_ids: List[str]
    from_date:   int
    to_date:     int


@app.post("/confirm/{booking_id}", status_code=202)
async def confirm_booking(booking_id: str, req: ConfirmRequest):
    """
    Single INSERT into outbox. That's the only side-effect in the entire system
    for this request — everything downstream (Kafka, BookingService, payment,
    ticket) is driven by the OutboxRelay reading this one row.

    ON CONFLICT (idempotency_key) DO NOTHING means:
    - First call with this booking_id → row written → relay picks it up.
    - Any retry             → conflict → row silently skipped → 202 returned.
    No locks, no extra SELECT, no confirmed-flag dance needed.
    """
    event = BookEvent(
        correlation_id=booking_id,
        vehicle_ids=req.vehicle_ids,
        user_id=req.user_id,
        from_date=req.from_date,
        to_date=req.to_date,
    )
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO outbox (topic, payload, idempotency_key)
            VALUES ($1, $2::jsonb, $3)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            Topic.BookingTopic.value,
            json.dumps(event.model_dump(mode="json")),
            booking_id,
        )
    return {"booking_id": booking_id, "status": "processing"}

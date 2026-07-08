"""
IngestionService — stateless HTTP entry point for booking requests (port 8003).

Idempotency-Key flow (Stripe-style):

  The CLIENT generates a UUID before making the request and sends it as:
      Idempotency-Key: <client-uuid>

  This key is used as both the booking_id and the outbox idempotency_key.

  POST /confirm
      • First call with key  → INSERT into outbox → relay picks it up → Kafka → done.
      • Retry with same key  → ON CONFLICT DO NOTHING → silently ignored → 202.
      • Crash before INSERT commits → transaction rolls back → retry inserts cleanly.
      • Crash after INSERT commits  → retry hits ON CONFLICT → idempotent.

  Why the client generates the key (not the server):
      If the server generated the key and the response never reached the client,
      the client has no key to retry with — it would send a new request, get a new
      key, and create a duplicate. With a client-generated key the client always
      knows the key before the first call, so every retry is identical.

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
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Header, HTTPException
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


class ConfirmRequest(BaseModel):
    user_id:     str
    vehicle_ids: List[str]
    from_date:   int
    to_date:     int


@app.post("/confirm", status_code=202)
async def confirm_booking(
    req: ConfirmRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    """
    Client must generate a UUID and send it as the Idempotency-Key header.
    The same key must be used on every retry for this booking attempt.
    The key becomes the booking_id for the entire downstream flow.

    ON CONFLICT (idempotency_key) DO NOTHING makes this endpoint safe to
    call any number of times — only the first committed INSERT has any effect.
    """
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    booking_id = idempotency_key

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
            idempotency_key,
        )
    return {"booking_id": booking_id, "status": "processing"}

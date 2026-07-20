"""
IngestionService — stateless HTTP entry point for booking requests (port 8003).

Flow:

  Step 1  POST /booking-intent
          Pure UUID factory. No DB, no Kafka. Just returns a booking_id.
          Client stores this ID and uses it for step 2.
          If the response never arrives → client retries → gets a new UUID → fine,
          because no state was written anywhere. The old UUID is simply never used.

  Step 2  POST /confirm/{booking_id}
          Publishes BookEvent directly to Kafka. No DB involved.
          Duplicates (client retries, Kafka redelivery) are handled downstream
          by BookingService's idempotent DB writes — same guarantee as before,
          without the outbox indirection or the DB bottleneck.

Run:
    uvicorn Zoom.CarRentalSystem.IngestionService.main:app --port 8003
"""
from dotenv import load_dotenv
load_dotenv()

import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from Zoom.CarRentalSystem.EventHandler.kafka_event_handler import KafkaEventHandler
from Zoom.EventStreamer.Event.event import BookEvent, VehicleBooking
from Zoom.EventStreamer.Topic.topic import Topic
from Zoom.CarRentalSystem.metrics import (
    metrics_endpoint,
    http_request_duration_seconds,
    ingestion_confirms_total,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "ingestion-service-group")

_event_handler: KafkaEventHandler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _event_handler
    _event_handler = KafkaEventHandler(KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
    await _event_handler.start()
    print(f"[IngestionService] Kafka producer connected at {KAFKA_BOOTSTRAP_SERVERS}")
    yield
    await _event_handler.stop()


app = FastAPI(title="IngestionService", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


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
# Step 1 — stateless UUID factory
# ---------------------------------------------------------------------------
@app.post("/booking-intent", status_code=200)
async def create_booking_intent():
    return {"booking_id": str(uuid.uuid4())}


# ---------------------------------------------------------------------------
# Step 2 — publish directly to Kafka
# ---------------------------------------------------------------------------
class ConfirmRequest(BaseModel):
    user_id:  str
    vehicles: List[VehicleBooking]   # each vehicle carries its own from_date / to_date


@app.post("/confirm", status_code=202)
async def confirm_booking(
    req: ConfirmRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    """
    Client generates a UUID once and sends it as Idempotency-Key on every retry.
    The key becomes booking_id for the entire downstream flow.
    Kafka may deliver duplicates — BookingService deduplicates on booking_id.
    """
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    event = BookEvent(
        correlation_id=idempotency_key,
        user_id=req.user_id,
        vehicles=req.vehicles,
    )
    await _event_handler.add(Topic.BookingTopic, event)
    ingestion_confirms_total.inc()
    return {"booking_id": idempotency_key, "status": "processing"}

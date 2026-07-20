"""
Shared Prometheus metrics for the Car Rental microservices.
Each service imports this module and gets its own Registry instance
(one Python process = one registry).

Usage in a FastAPI service:
    from Zoom.CarRentalSystem.metrics import registry, <metric>
    app.add_route("/metrics", metrics_endpoint)

Usage in a plain asyncio service (BookingService / PaymentService):
    from Zoom.CarRentalSystem.metrics import start_metrics_server
    asyncio.create_task(start_metrics_server(port=8002))
"""
import asyncio
from fastapi.responses import PlainTextResponse
from fastapi import Request

from Prometheus.Registry import Registry
from Prometheus.Metrics.Counter import Counter
from Prometheus.Metrics.Gauge import Gauge
from Prometheus.Metrics.Histogram import Histogram

# ---------------------------------------------------------------------------
# One registry per process (singleton)
# ---------------------------------------------------------------------------
registry = Registry()

# ---------------------------------------------------------------------------
# EventStreamer metrics
# ---------------------------------------------------------------------------
streamer_messages_put_total = registry.register(Counter(
    name="streamer_messages_put_total",
    help="Total messages published to EventStreamer",
    label_names=("topic",),
))

streamer_messages_get_total = registry.register(Counter(
    name="streamer_messages_get_total",
    help="Total messages consumed from EventStreamer",
    label_names=("topic",),
))

streamer_queue_timeouts_total = registry.register(Counter(
    name="streamer_queue_timeouts_total",
    help="Total queue put/get timeouts (back-pressure signal)",
    label_names=("topic", "operation"),
))

streamer_queue_size = registry.register(Gauge(
    name="streamer_queue_size",
    help="Current number of messages waiting in each topic queue",
    label_names=("topic",),
))

# ---------------------------------------------------------------------------
# TicketService / Gateway metrics
# ---------------------------------------------------------------------------
booking_requests_total = registry.register(Counter(
    name="booking_requests_total",
    help="Total POST /book requests accepted",
))

tickets_confirmed_total = registry.register(Counter(
    name="tickets_confirmed_total",
    help="Total booking sagas that reached ticket-confirmed state",
))

result_store_size = registry.register(Gauge(
    name="result_store_size",
    help="Number of confirmed tickets held in TicketService memory",
))

http_request_duration_seconds = registry.register(Histogram(
    name="http_request_duration_seconds",
    help="HTTP request processing time in seconds",
    label_names=("method", "path"),
))

# ---------------------------------------------------------------------------
# BookingService metrics
# ---------------------------------------------------------------------------
booking_events_processed_total = registry.register(Counter(
    name="booking_events_processed_total",
    help="Total events processed by BookingService",
    label_names=("event_type",),
))

booking_slots_pending = registry.register(Gauge(
    name="booking_slots_pending",
    help="Current number of vehicle-date slots in pending state",
))

booking_slots_booked = registry.register(Gauge(
    name="booking_slots_booked",
    help="Current number of vehicle-date slots in booked state",
))

booking_lock_failures_total = registry.register(Counter(
    name="booking_lock_failures_total",
    help="Times a booking failed because the vehicle lock could not be acquired",
    label_names=("event_type",),
))

event_processing_duration_seconds = registry.register(Histogram(
    name="event_processing_duration_seconds",
    help="Time to fully process a booking event (seconds)",
    label_names=("event_type",),
))

# ---------------------------------------------------------------------------
# PaymentService metrics
# ---------------------------------------------------------------------------
payments_processed_total = registry.register(Counter(
    name="payments_processed_total",
    help="Total payment attempts processed",
    label_names=("result",),   # initiated | error
))

payment_duration_seconds = registry.register(Histogram(
    name="payment_duration_seconds",
    help="Time spent processing a payment (seconds)",
))

# ---------------------------------------------------------------------------
# IngestionService / OutboxRelay metrics
# ---------------------------------------------------------------------------
ingestion_confirms_total = registry.register(Counter(
    name="ingestion_confirms_total",
    help="Total POST /confirm requests accepted into the outbox",
))

outbox_relay_published_total = registry.register(Counter(
    name="outbox_relay_published_total",
    help="Total outbox rows this process's relay has published to Kafka",
))

outbox_relay_batch_size = registry.register(Histogram(
    name="outbox_relay_batch_size",
    help="Number of rows published per relay poll cycle",
    buckets=(0, 1, 5, 10, 25, 50, 100, 250, 500),
))


# ---------------------------------------------------------------------------
# FastAPI helper — add to any FastAPI app
# ---------------------------------------------------------------------------
async def metrics_endpoint(request: Request):
    return PlainTextResponse(registry.serialize())


# ---------------------------------------------------------------------------
# Standalone metrics HTTP server — for plain asyncio services
# ---------------------------------------------------------------------------
async def start_metrics_server(port: int):
    """
    Lightweight HTTP server that only serves GET /metrics.
    Used by BookingService and PaymentService which have no FastAPI app.
    """
    import re

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=5)
            request_line = data.decode(errors="replace").split("\r\n")[0]
            if re.match(r"GET /metrics", request_line):
                body = registry.serialize()
                response = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: text/plain; version=0.0.4\r\n"
                    f"Content-Length: {len(body.encode())}\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    + body
                )
            else:
                response = "HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n"
            writer.write(response.encode())
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    print(f"[metrics] Serving /metrics on :{port}")
    async with server:
        await server.serve_forever()
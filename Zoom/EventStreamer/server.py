import time
from fastapi import FastAPI, Request
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Orchestrator.orchestrator import EventStreamer
from Zoom.EventStreamer.Topic.topic import Topic
# from Zoom.CarRentalSystem.metrics import (
#     metrics_endpoint,
#     streamer_messages_put_total,
#     streamer_messages_get_total,
#     streamer_queue_size,
#     http_request_duration_seconds,
# )

app = FastAPI()
manager = EventStreamer()


@app.middleware("http")
async def track_latency(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    # if request.url.path != "/metrics":
        # http_request_duration_seconds.labels(
            # method=request.method, path=request.url.path
        # ).observe(time.perf_counter() - start)
    return response


@app.post("/put/{topic}")
async def put(topic: Topic, event: AnyEvent):
    result = await manager.add(topic, event)
    # streamer_messages_put_total.labels(topic=topic.value).inc()
    # streamer_queue_size.labels(topic=topic.value).inc()
    return result


@app.get("/get/{topic}")
async def get(topic: Topic):
    result = await manager.get(topic)
    # if result is not None:
    #     streamer_messages_get_total.labels(topic=topic.value).inc()
    #     streamer_queue_size.labels(topic=topic.value).dec()
    return result


@app.get("/health")
async def health():
    """Health check endpoint for container readiness probes"""
    return {"status": "healthy", "service": "event-streamer"}

# app.add_route("/metrics", metrics_endpoint)

# uvicorn Zoom.EventStreamer.server:app --port 8000


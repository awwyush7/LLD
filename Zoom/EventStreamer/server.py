from fastapi import Body, FastAPI
from Zoom.EventStreamer.Event.event import AnyEvent
from Zoom.EventStreamer.Orchestrator.orchestrator import EventStreamer
from Zoom.EventStreamer.Topic.topic import Topic

app = FastAPI()

manager = EventStreamer()


@app.post("/put/{topic}")
async def put(topic:Topic, event: AnyEvent):
    return await manager.add(topic, event)

@app.get("/get/{topic}")
async def get(topic: Topic):
    return await manager.get(topic)


# uvicorn Zoom.EventStreamer.server:app --port 8000


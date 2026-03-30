from fastapi import Body, FastAPI
from pydantic import BaseModel
from Zoom.EventStreamer.Orchestrator.orchestrator import EventStreamer
app = FastAPI()

manager = EventStreamer()

class EventRequest(BaseModel):
    event: str

@app.post("/put/{topic}")
async def put(topic:str, event: EventRequest):
    return await manager.add(topic, event)

@app.get("/get/{topic}")
async def get(topic):
    return await manager.get(topic)




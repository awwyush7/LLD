from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.EventStreamer.Topic.topic import Topic
import asyncio

async def put_task(handler : EventHandler):
    response = await handler.put_task(Topic.BookingTopic,"test_task")
    print(response)

    response = await handler.get_tasks("Bookin")
    print(response)

if __name__ == "__main__":
    handler = EventHandler("http://127.0.0.1:8000")
    asyncio.run(put_task(handler))
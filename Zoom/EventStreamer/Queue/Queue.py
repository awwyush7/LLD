from asyncio import Queue
import asyncio

class EventQueue:
    def __init__(self):
        self.__queue = Queue(1500)

    async def add(self, event):
        try:
            return await asyncio.wait_for(self.__queue.put(event), 30)
        except asyncio.TimeoutError:
            return None
    
    async def get(self):
        try:
            data = await asyncio.wait_for(self.__queue.get(), 30)
            return data
        except asyncio.TimeoutError:
            return None


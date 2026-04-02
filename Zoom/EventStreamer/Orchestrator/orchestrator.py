from Zoom.EventStreamer.Queue.Queue import EventQueue
from typing import Dict

class EventStreamer:
    def __init__(self):
        self.__events = {}

    async def add(self, topic, event):
        if topic not in self.__events:
            self.__events[topic] = EventQueue()
        return await self.__events[topic].add(event)

    async def get(self, topic):
        if topic not in self.__events:
            self.__events[topic] = EventQueue()
        return await self.__events[topic].get()
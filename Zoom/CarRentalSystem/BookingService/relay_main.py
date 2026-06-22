"""
Outbox Relay — separate process that publishes committed outbox rows to the EventStreamer.
Must run independently of BookingService so it survives BookingService crashes.

Run:
    python3 -m Zoom.CarRentalSystem.BookingService.relay_main
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import os

from Zoom.CarRentalSystem.Repository.Postgres.db import get_pool, close_pool
from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.CarRentalSystem.BookingService.outbox_relay import OutboxRelay

STREAMER_URL = os.getenv("STREAMER_URL", "http://127.0.0.1:8000")


async def main():
    pool = await get_pool()
    event_handler = EventHandler(STREAMER_URL)
    relay = OutboxRelay(pool, event_handler)
    try:
        await relay.run()
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())

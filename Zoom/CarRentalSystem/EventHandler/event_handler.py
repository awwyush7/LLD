import httpx


class EventHandler:
    def __init__(self, streamer_url: str):
        self.url = streamer_url
        self.client = httpx.AsyncClient(timeout=40.0)

    async def get_tasks(self, topic: str):
        while True:
            try:
                response = await self.client.get(f"{self.url}/get/{topic}")
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        return data
                    continue
            except httpx.ReadTimeout:
                continue

    async def add(self, topic, event):
        response = await self.client.post(
            f"{self.url}/put/{topic.value}",
            json=event.model_dump(mode="json"),
        )
        response.raise_for_status()

    async def publish_raw(self, topic: str, payload: dict):
        """Used by the outbox relay — publishes a pre-serialised event dict."""
        response = await self.client.post(
            f"{self.url}/put/{topic}",
            json=payload,
        )
        response.raise_for_status()

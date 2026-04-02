import httpx
import asyncio

class EventHandler:
    def __init__(self, streamer_url: str):
        self.url = streamer_url
        self.client = httpx.AsyncClient(timeout=40.0) # Timeout > Server Long Poll

    async def get_tasks(self, topic: str):
        """
        Listens for the next available task from the streamer.
        """
        while True:
            try:
                # 1. Hit the endpoint you defined
                response = await self.client.get(f"{self.url}/get/{topic}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        return data
                    
                    continue 

            except httpx.ReadTimeout:
                # This is normal for long polling. The server held the 
                # connection but no message arrived. Just loop again.
                continue

    async def add(self, topic, event):
        response = await self.client.post(
            f"{self.url}/put/{topic.value}",
            json = event.model_dump(mode = "json")
        )
        response.raise_for_status()
        return response.json
    
        while True:
            try:
                response = await self.client.post(f"{self.url}/put/{topic}", json={"event": task})
                if response.status_code == 200:
                    return 
            except httpx.ReadTimeout:
                    # This is normal for long polling. The server held the 
                    # connection but no message arrived. Just loop again.
                    continue
            except Exception as e:
                raise e

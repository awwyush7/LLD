"""
Mock Stripe service — mimics Stripe Checkout.

POST /v1/checkout/sessions
  body: { booking_id, amount, user_id }
  response: { session_id, redirect_url }
  side-effect: after 3 seconds, POSTs to WEBHOOK_URL with success (50%) or failure (50%)

GET /pay/{session_id}
  Fake hosted payment page (in real Stripe this is stripe.com/pay/cs_xxx).

Run:
    uvicorn Zoom.CarRentalSystem.MockStripe.main:app --port 8003
"""
from dotenv import load_dotenv
load_dotenv()

import asyncio
import os
import random
import uuid

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:8001/webhook/payment")
SELF_URL = os.getenv("SELF_URL", "http://localhost:8003")

app = FastAPI(title="MockStripe")


class CheckoutRequest(BaseModel):
    booking_id: str
    amount: float
    user_id: str


@app.post("/v1/checkout/sessions")
async def create_checkout_session(req: CheckoutRequest):
    session_id = f"cs_{uuid.uuid4().hex}"
    redirect_url = f"{SELF_URL}/pay/{session_id}"

    # Simulate Stripe firing the webhook after the user "pays"
    asyncio.create_task(_fire_webhook(session_id, req.booking_id))

    return {"session_id": session_id, "redirect_url": redirect_url}


@app.get("/pay/{session_id}")
async def fake_payment_page(session_id: str):
    """In real Stripe, the user would see the hosted payment UI here."""
    return {"message": f"[MockStripe] Payment page for session {session_id}. User would enter card details here."}


@app.get("/health")
async def health():
    return {"status": "ok"}


async def _fire_webhook(session_id: str, booking_id: str) -> None:
    """Wait 3 seconds (simulates user filling card form), then call our webhook."""
    await asyncio.sleep(3)

    success = random.random() < 0.5
    payload = {
        "session_id": session_id,
        "booking_id": booking_id,
        "status": "paid" if success else "failed",
        "reason": None if success else "Card declined",
    }

    print(f"[MockStripe] Firing webhook  booking={booking_id[:8]}  status={payload['status']}")
    async with httpx.AsyncClient() as client:
        try:
            await client.post(WEBHOOK_URL, json=payload, timeout=5.0)
        except Exception as e:
            print(f"[MockStripe] Webhook delivery failed: {e}")

"""
TicketService — API Gateway + Ticket Consumer
Runs on port 8001. Serves the UI, accepts bookings, polls for tickets.

Run from C:\\Learning\\lld_new\\LLD:
    $env:PYTHONPATH = "C:\\Learning\\lld_new\\LLD"
    uvicorn Zoom.CarRentalSystem.TicketService.main:app --port 8001

Requires EventStreamer running on port 8000, BookingService and
PaymentService workers running separately.
"""
import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, TypeAdapter

from Zoom.CarRentalSystem.EventHandler.event_handler import EventHandler
from Zoom.EventStreamer.Event.event import BookEvent, AnyEvent
from Zoom.EventStreamer.Topic.topic import Topic

STREAMER_URL = "http://127.0.0.1:8000"

result_store: dict = {}  # booking_id → ticket dict
event_handler = EventHandler(STREAMER_URL)
adapter = TypeAdapter(AnyEvent)


# ---------------------------------------------------------------------------
# Background consumer — listens on TicketTopic, writes to result_store
# ---------------------------------------------------------------------------
async def ticket_consumer():
    print("[TicketService] Listening on TicketTopic...")
    while True:
        try:
            raw = await event_handler.get_tasks(Topic.TicketTopic.value)
            event = adapter.validate_python(raw)
            print(f"[TicketService] Ticket confirmed booking={event.booking_id[:8]}")
            result_store[event.booking_id] = {
                "booking_id": event.booking_id,
                "user_id": event.user_id,
                "vehicle_ids": event.vehicle_ids,
                "from_date": event.from_date,
                "to_date": event.to_date,
                "status":   "confirmed",
            }
        except Exception as exc:
            print(f"[TicketService] consumer error: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(ticket_consumer())
    yield
    task.cancel()


app = FastAPI(title="TicketService / Gateway", lifespan=lifespan)


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
class BookRequest(BaseModel):
    vehicle_ids: List[str]
    user_id: str
    from_date: int
    to_date: int


@app.post("/book")
async def book(req: BookRequest):
    booking_id = str(uuid.uuid4())
    await event_handler.add(
        Topic.BookingTopic,
        BookEvent(
            correlation_id=booking_id,
            vehicle_ids=req.vehicle_ids,
            user_id=req.user_id,
            from_date=req.from_date,
            to_date=req.to_date,
        ),
    )
    return {"booking_id": booking_id, "status": "processing"}


@app.get("/ticket/{booking_id}")
def get_ticket(booking_id: str):
    return result_store.get(booking_id) or {"status": "pending"}


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Car Rental System — LLD Demo</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-8 font-sans">

  <div class="max-w-5xl mx-auto">
    <h1 class="text-3xl font-bold mb-1">🚗 Car Rental System</h1>
    <p class="text-gray-400 mb-1 text-sm">Event-driven Microservices — BookEvent → PaymentRequestEvent → PaymentSuccessEvent → GenerateTicketEvent</p>
    <p class="text-gray-600 mb-8 text-xs">Distributed mode: EventStreamer :8000 · BookingService · PaymentService · TicketService :8001</p>

    <div class="grid grid-cols-2 gap-8">


      <!-- LEFT: vehicles -->

      <div>

        <h2 class="font-semibold text-lg mb-3">Vehicles</h2>

        <div class="grid grid-cols-2 gap-3 mb-4">

          <div class="vehicle-card bg-gray-800 border-2 border-gray-700 rounded-lg p-3 cursor-pointer hover:border-blue-500 transition-colors select-none" onclick="toggleVehicle(this,'V001')">

            <div class="font-bold">V001</div><div class="text-xs text-gray-400 mt-1">KA-01-1001</div><div class="text-xs text-gray-500">2022</div>

          </div>

          <div class="vehicle-card bg-gray-800 border-2 border-gray-700 rounded-lg p-3 cursor-pointer hover:border-blue-500 transition-colors select-none" onclick="toggleVehicle(this,'V002')">

            <div class="font-bold">V002</div><div class="text-xs text-gray-400 mt-1">KA-01-1002</div><div class="text-xs text-gray-500">2022</div>

          </div>

          <div class="vehicle-card bg-gray-800 border-2 border-gray-700 rounded-lg p-3 cursor-pointer hover:border-blue-500 transition-colors select-none" onclick="toggleVehicle(this,'V003')">

            <div class="font-bold">V003</div><div class="text-xs text-gray-400 mt-1">KA-01-1003</div><div class="text-xs text-gray-500">2022</div>

          </div>

          <div class="vehicle-card bg-gray-800 border-2 border-gray-700 rounded-lg p-3 cursor-pointer hover:border-blue-500 transition-colors select-none" onclick="toggleVehicle(this,'V004')">

            <div class="font-bold">V004</div><div class="text-xs text-gray-400 mt-1">KA-01-1004</div><div class="text-xs text-gray-500">2022</div>

          </div>

          <div class="vehicle-card bg-gray-800 border-2 border-gray-700 rounded-lg p-3 cursor-pointer hover:border-blue-500 transition-colors select-none" onclick="toggleVehicle(this,'V005')">

            <div class="font-bold">V005</div><div class="text-xs text-gray-400 mt-1">KA-01-1005</div><div class="text-xs text-gray-500">2022</div>

          </div>

        </div>

        <p id="selectionLabel" class="text-sm text-gray-400 italic">Click cards to select vehicles</p>

      </div>



      <!-- RIGHT: form -->

      <div>

        <h2 class="font-semibold text-lg mb-3">New Booking</h2>

        <form id="bookForm" class="space-y-4">

          <div>

            <label class="block text-sm text-gray-400 mb-1">User ID</label>

            <input id="userId" value="alice" class="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">

          </div>

          <div class="grid grid-cols-2 gap-4">

            <div>

              <label class="block text-sm text-gray-400 mb-1">From (day)</label>

              <input id="fromDate" type="number" value="1" min="1"

                class="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">

            </div>

            <div>

              <label class="block text-sm text-gray-400 mb-1">To (day)</label>

              <input id="toDate" type="number" value="3" min="1"

                class="w-full bg-gray-800 border border-gray-700 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500">

            </div>

          </div>

          <button type="submit"

            class="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2 rounded transition-colors">

            Book Now

          </button>

        </form>

      </div>

    </div>



    <!-- EVENT LOG -->

    <div class="mt-10">

      <div class="flex items-center justify-between mb-3">

        <h2 class="font-semibold text-lg">Event Log</h2>

        <button onclick="document.getElementById('log').innerHTML=''"

          class="text-xs text-gray-500 hover:text-gray-300 transition-colors">Clear</button>

      </div>

      <div id="log" class="bg-gray-900 border border-gray-800 rounded-lg p-4 min-h-32 space-y-1 font-mono text-sm overflow-y-auto max-h-72"></div>

    </div>

  </div>



<script>

  let selected = [];

  const COLORS = {

    book: 'text-blue-400', payment: 'text-purple-400',

    success: 'text-green-400', error: 'text-red-400',

    info: 'text-gray-400', ticket: 'text-yellow-300',

  };



  function log(msg, kind = 'info') {

    const el = document.createElement('div');

    el.className = COLORS[kind] || 'text-white';

    el.textContent = `[${new Date().toLocaleTimeString()}]  ${msg}`;

    document.getElementById('log').prepend(el);

  }



  function toggleVehicle(el, id) {

    if (selected.includes(id)) {

      selected = selected.filter(v => v !== id);

      el.classList.remove('border-blue-500', 'bg-gray-700');

      el.classList.add('border-gray-700');

    } else {

      selected.push(id);

      el.classList.add('border-blue-500', 'bg-gray-700');

      el.classList.remove('border-gray-700');

    }

    document.getElementById('selectionLabel').textContent =

      selected.length ? `Selected: ${selected.join(', ')}` : 'Click cards to select vehicles';

  }



  function pollTicket(bookingId) {

    const short = bookingId.slice(0, 8);

    log(`Polling for ticket [${short}…]`, 'payment');

    let attempts = 0;

    const timer = setInterval(async () => {

      if (++attempts > 20) {   // 20 x 800 ms = 16 s timeout

        clearInterval(timer);

        log(`⏱ No ticket after 16s — check that all services are running`, 'error');

        return;

      }

      const data = await (await fetch(`/ticket/${bookingId}`)).json();

      if (data.status === 'confirmed') {

        clearInterval(timer);

        log(`✅ TICKET CONFIRMED  booking=${short}  vehicles=${data.vehicle_ids.join(',')}  days=${data.from_date}-${data.to_date}  user=${data.user_id}`, 'ticket');

      } else if (data.status === 'failed') {

        clearInterval(timer);

        log(`❌ BOOKING FAILED  ${data.reason}`, 'error');

      }

    }, 800);

  }



  document.getElementById('bookForm').addEventListener('submit', async (e) => {

    e.preventDefault();

    if (!selected.length) { log('Select at least one vehicle first.', 'error'); return; }



    const body = {

      vehicle_ids: selected,

      user_id:     document.getElementById('userId').value.trim() || 'guest',

      from_date:   parseInt(document.getElementById('fromDate').value),

      to_date:     parseInt(document.getElementById('toDate').value),

    };

    if (body.from_date > body.to_date) { log('from_date must be ≤ to_date', 'error'); return; }



    log(`→ [1/4] BookEvent  vehicles=${body.vehicle_ids.join(',')}  days=${body.from_date}-${body.to_date}  user=${body.user_id}`, 'book');

    const res = await fetch('/book', {

      method: 'POST', headers: { 'Content-Type': 'application/json' },

      body: JSON.stringify(body),

    });

    if (!res.ok) { log(`❌ ${(await res.json()).detail}`, 'error'); return; }



    const data = await res.json();

    log(`→ [2/4] PaymentRequestEvent  booking=${data.booking_id.slice(0,8)}…`, 'payment');

    log(`→ [3/4] PaymentSuccessEvent (pending payment processor…)`, 'payment');

    pollTicket(data.booking_id);

  });

</script>

</body>

</html>"""

@app.get("/", response_class=HTMLResponse)
def ui():
    return HTML_UI


# uvicorn Zoom.CarRentalSystem.TicketService.main:app --port 8001
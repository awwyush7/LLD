# Car Rental System — Event-Driven Microservices (LLD)

A low-level design implementation of a Car Rental System built with **Python**, **FastAPI**, and **asyncio**. The system is fully event-driven: services communicate exclusively through an in-house event streamer (long-poll queue), with no direct service-to-service HTTP calls.

---

## Architecture

```
User / Browser
      │
      │  POST /book     GET /ticket/{id}
      ▼
┌─────────────────────────────────┐
│      TicketService  :8001       │  ← Only public-facing service
│  • Serves UI                    │
│  • API Gateway for bookings     │
│  • Consumes TicketTopic         │
│  • result_store for polling     │
└───────────────┬─────────────────┘
                │ publishes BookEvent
                ▼
┌─────────────────────────────────┐
│      EventStreamer  :8000       │  ← Message broker
│  • One asyncio.Queue per topic  │
│  • POST /put/{topic}  publish   │
│  • GET  /get/{topic}  consume   │
└───┬───────────────┬─────────────┘
    │               │
 BookingTopic   PaymentTopic   TicketTopic
    │               │               │
    ▼               ▼               ▼
BookingService  PaymentService  TicketService
```

### Event Flow (Happy Path)

```
POST /book
  └─► BookEvent ──► BookingTopic
                         └─► BookingService
                               • acquire locks (sorted ID order)
                               • pre-check + re-check availability (double-checked locking)
                               • mark slots "pending"
                               • release locks
                               └─► PaymentRequestEvent ──► PaymentTopic
                                                                └─► PaymentService
                                                                      • charge user (stub)
                                                                      └─► PaymentSuccessEvent ──► BookingTopic
                                                                                                       └─► BookingService
                                                                                                             • mark slots "booked"
                                                                                                             └─► GenerateTicketEvent ──► TicketTopic
                                                                                                                                              └─► TicketService
                                                                                                                                                    • write to result_store
GET /ticket/{booking_id}  ──► {"status": "confirmed", ...}
```

---

## Key Design Decisions

### Concurrency & Locking
- Each `Vehicle` object carries a `threading.Lock`.
- Vehicles are always locked in **sorted ID order** to prevent deadlocks when two concurrent requests share vehicles.
- **Double-checked locking**: availability is checked once before acquiring any locks (fast-fail), then re-checked under the lock (correctness guard against races).
- Locks are released **before** any `await` call, so a slow queue never holds a vehicle hostage.

### Event Schema
- All events inherit from a `BaseModel` with `event_id`, `correlation_id`, and `timestamp`.
- The `correlation_id` (the `booking_id`) flows through every event in the chain — this is how `/ticket/{id}` can look up the result across 4 processes.
- Pydantic **discriminated unions** (`type` literal field) allow typed deserialization at the consumer side without `if/else` on raw dicts.

### Long Polling
- The EventStreamer holds a `asyncio.Queue(10)` per topic.
- Consumers call `GET /get/{topic}` which blocks up to 30 seconds. On timeout, they loop. No Redis or Kafka needed for the demo.

---

## Project Structure

```
LLD/Zoom/
├── EventStreamer/
│   ├── server.py           # FastAPI broker — /put and /get endpoints
│   ├── Event/event.py      # All Pydantic event schemas + AnyEvent discriminated union
│   ├── Orchestrator/       # EventStreamer class (per-topic queue manager)
│   ├── Queue/Queue.py      # asyncio.Queue wrapper with timeout
│   └── Topic/topic.py      # Topic enum (BookingTopic, PaymentTopic, TicketTopic)
│
└── CarRentalSystem/
    ├── EventHandler/       # HTTP client that wraps /put and /get calls
    ├── BookingService/
    │   ├── main.py         # Worker entrypoint — seeds vehicles, listens on BookingTopic
    │   ├── booking_service.py  # Core locking + slot management logic
    │   └── orchestrator.py    # Deserializes events, dispatches to handlers
    ├── PaymentService/
    │   ├── main.py         # Worker entrypoint — listens on PaymentTopic
    │   └── payment_service.py # Charge stub, publishes success/failure
    ├── TicketService/
    │   └── main.py         # FastAPI app — UI, /book gateway, TicketTopic consumer
    ├── VehicleService/
    │   ├── vehicle_service.py  # In-memory vehicle store
    │   └── Vehicle/vehicle.py  # Vehicle model with threading.Lock
    └── demo.py             # All-in-one single-process demo (no separate services needed)
```

---

## Running the System

### Prerequisites

```bash
pip install fastapi uvicorn httpx pydantic
```

### Option A — All-in-one demo (quickest)

```bash
cd LLD
export PYTHONPATH=$(pwd)          # Mac/Linux
# $env:PYTHONPATH = "$PWD"        # Windows PowerShell

uvicorn Zoom.CarRentalSystem.demo:app --port 8001
```

Open **http://localhost:8001**

---

### Option B — Full distributed mode (4 terminals)

All commands run from `LLD/` with `PYTHONPATH` set as above.

**Terminal 1 — EventStreamer (start first)**
```bash
uvicorn Zoom.EventStreamer.server:app --port 8000
```

**Terminal 2 — BookingService**
```bash
python -m Zoom.CarRentalSystem.BookingService.main
```

**Terminal 3 — PaymentService**
```bash
python -m Zoom.CarRentalSystem.PaymentService.main
```

**Terminal 4 — TicketService + UI**
```bash
uvicorn Zoom.CarRentalSystem.TicketService.main:app --port 8001
```

Open **http://localhost:8001**

---

## Stress Testing (Locust)

```bash
pip install locust
cd LLD
locust -f Zoom/locustfile.py --host http://127.0.0.1:8001
```

Open **http://localhost:8089** to configure users and ramp rate.

---

## Requirements

| # | Requirement |
|---|---|
| 1 | Users can search for vehicles based on availability |
| 2 | Users can book one or more vehicles for a date range |
| 3 | Users can cancel bookings |
| 4 | Users can pay via their preferred method (CC/DC/UPI — stub) |

---

## Non-Functional Properties Demonstrated

- **Thread safety** — sorted lock acquisition order + double-checked locking
- **Async correctness** — locks never held across `await` boundaries
- **Loose coupling** — services share only the event schema, not code or databases
- **Observability** — `correlation_id` on every event enables end-to-end tracing
- **Resilience** — consumers retry on long-poll timeout; `result_store` prevents polling from hanging on failure
1) Availability in general but for orders/bookings consistency.
2) Low latency serach <500ms>
3) Fault Tolerant
4) Durable
5) Partition Tolerant
6) Idempotent Requests.
etc.

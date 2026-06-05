import time 
import uuid
from locust import HttpUser, task, between, events


#Track end-to-end booking confirmation latency in ms. 
booking_start_times = {}

@events.request.add_listener
def on_request(request_type, name, response_time, response_length, response, context, exception, **kwargs):
    # Keep default Locust request metrics and also annotate business failures.
    if exception:
        return
    


class TicketGatewayUser(HttpUser):
    # Small think time keeps pressure high while still realistic.
    wait_time = between(0.01,0.2)

    @task(4)
    def book_and_poll_unique_slot(self):
        """
        High-throughput flow with low contention:
        - POST /book with mostly unique date windows
        -Poll/ticket/(booking_id) until confirmed/timeout
        """
        request_id = str(uuid.uuid4())
        user_id = f"u-{request_id[:8]}"
        base = int(time.time()) % 100000
        from_date = base + (hash(request_id) % 20000)
        to_date = from_date + 1
        vehicle_id = f"V{((hash(request_id) % 5) + 1):03d}"

        payload = {
        "vehicle_ids": [vehicle_id],
        "user_id": user_id,
        "from_date": from_date,
        "to_date" : to_date,
        }

        with self.client.post("/book",json=payload, name="POST /book",catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"book failed status={resp.status_code}")
                return
            
            body = resp.json()
            booking_id = body.get("booking_id")
            if not booking_id:
                resp.failure("book response missing booking_id")
                return
            resp.success()
            
        booking_start_times[booking_id] = time.time()
        self._poll_ticket_until_done(booking_id, timeout_s=20)

    @task(1)
    def book_with_contention(self):
        """
        Intentional contention test:
        multiple users attempt same slot to validate no duplicate confirms.
        """
        payload = {
        "vehicle_ids": ["V001"],
        "user_id": f"hot-{uuid.uuid4().hex[:6]}",
        "from_date": 77777,
        "to_date": 77779,
        }

        with self.client.post("/book", json=payload, name="POST /book (contended)", catch_response=True) as resp:
            if resp.status_code != 200:
                resp. failure(f"contended book failed status=(resp. status_code).")
                return
            body = resp.json()
            booking_id = body.get("booking_id")
            if not booking_id:
                resp.failure("contended response missing booking_id")
                return
            resp.success()

        booking_start_times[booking_id] = time.time()
        self._poll_ticket_until_done(booking_id, timeout_s=20, metric_name="GET /ticket (contended)")

    def _poll_ticket_until_done(self, booking_id: str, timeout_s: int = 20, metric_name : str =  "GET/ticket"):
        deadline = time. time() + timeout_s
        while time.time() < deadline:
            with self.client.get(f"/ticket/{booking_id}", name=metric_name, catch_response=True) as resp:
                if resp.status_code != 200:
                    resp. failure(f"ticket status={resp.status_code} ")
                    return
                
                data = resp. json()
                status = data.get("status")
        
                if status == "confirmed":
                    start = booking_start_times. pop(booking_id, None)
                    if start is not None:
                        e2e_ms = (time.time() - start) * 1000
                        # Add custom synthetic metric as request event.
                        events.request.fire(
                            request_type="E2E", 
                            name="Book->Confirm latency", 
                            response_time=e2e_ms, 
                            response_length=0, 
                            response=resp, 
                            context = {},
                            exception=None,
                        )
                    resp.success()
                    return
                
                # pending is expected while saga completes
                if status == "pending":
                    resp.success()
                else:
                    resp.failure(f"unexpected ticket status = {status}")
                    return
                
            time.sleep(0.2)

        # timeout means not confirmed in SLA window
        events. request.fire(
        request_type="E2E",
        name= "Book->Confirm timeout", 
        response_time=timeout_s * 1000, 
        response_length=0, 
        response=None,
        context={},
        exception=TimeoutError ("ticket confirmation timeout"),
        )

# locust -f Zoom/locustfile.py --host http://127.0.0.1:8001
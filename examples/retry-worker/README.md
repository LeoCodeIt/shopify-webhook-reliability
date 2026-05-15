# Retry Worker — Reference Implementation

A complete async worker that reads from Redis Streams, processes webhook jobs against a target system, handles failures with exponential backoff, and moves exhausted jobs to the dead letter queue.

---

## What This Implements

- Redis Streams consumer with `XREADGROUP` and consumer groups
- Error classification: retriable vs. non-retriable
- Exponential backoff with full jitter (`random.uniform(0, min(cap, base * 2^attempt))`)
- `Retry-After` header handling for rate-limited requests
- Job state tracking in PostgreSQL (`webhook_events` table)
- DLQ insertion with full error history on exhausted retries or non-retriable errors
- Non-blocking stream read with 5-second block timeout

---

## Error Classification

The worker classifies every failure before deciding whether to retry:

| HTTP Status | Classification | Reason |
|---|---|---|
| 422 | Non-retriable | Payload is malformed — retrying will not fix it |
| 429 | Retriable | Rate limited — use `Retry-After` delay if present |
| 500, 502, 503, 504 | Retriable | Server error — target may recover |
| Other 4xx | Non-retriable | Client error — payload or auth problem |
| Connection/timeout | Retriable | Transient network issue |
| Unknown | Retriable | Default to retry — DLQ catches persistent failures |

Adjust the classification logic in `classify_error()` for your specific target system's error contract.

---

## Backoff Formula

```python
delay = random.uniform(0, min(MAX_DELAY_SECONDS, BASE_DELAY * 2^attempt))
```

Full jitter prevents thundering herd: when multiple workers have been retrying since the same outage, they will not all retry simultaneously when the outage resolves.

At `BASE_DELAY_SECONDS = 2.0` and `MAX_DELAY_SECONDS = 300.0`:

| Attempt | Max delay |
|---|---|
| 1 | 4s |
| 2 | 8s |
| 3 | 16s |
| 4 | 32s |
| 5 (max) → DLQ | — |

---

## Replacing the Target System Stub

Replace `process_at_target()` with your actual ERP/WMS/CRM API call:

```python
async def process_at_target(topic: str, payload: dict) -> None:
    if topic == "orders/create":
        response = await erp_client.post("/orders", json=payload)
        if not response.is_success:
            raise FakeTargetSystemError(response.text, http_status=response.status_code)
    elif topic == "inventory_levels/update":
        response = await wms_client.put("/inventory", json=payload)
        # ...
```

Attach the HTTP status code to the exception so `classify_error()` can make the right decision.

---

## Consumer Groups

The worker uses Redis Streams consumer groups so that:

- Multiple worker instances can run concurrently — each message is delivered to one consumer
- Messages that crash a worker stay in the Pending Entries List (PEL) and can be claimed by another consumer
- `XACK` is called only after the job is fully handled (success or DLQ insertion)

Run multiple workers by changing `CONSUMER_NAME` per instance:

```bash
CONSUMER_NAME=worker-1 python worker.py
CONSUMER_NAME=worker-2 python worker.py
```

---

## Running

```bash
pip install -r requirements.txt

# PostgreSQL and Redis must be running
# The webhook_events and dead_letter_queue tables must exist

python worker.py
```

The worker runs indefinitely. Use a process supervisor (systemd, Docker restart policy) in production.

---

## What This Does Not Include

- Metrics (Prometheus) — see `docs/observability.md`
- Worker concurrency limit relative to ERP rate limits — see `docs/production-considerations.md`
- Automatic PEL recovery for crashed workers (XCLAIM / XAUTOCLAIM)
- Schema migrations (the `webhook_events` table is created by the receiver)

# shopify-webhook-reliability

A technical reference architecture for designing reliable Shopify webhook handlers. Covers idempotency, async processing, retry strategies, dead letter queues, and replay tooling.

> **This is a reference architecture and educational implementation.** It documents patterns, design decisions, and trade-offs — not a production-ready framework. Adapt the patterns to your infrastructure and requirements.

---

## Overview

Shopify webhooks are the primary event delivery mechanism for integration systems. They notify your infrastructure when orders are created, inventory changes, fulfillments are updated, and dozens of other state transitions occur.

The delivery model has a property that most integration handlers do not account for: **webhooks should be treated as at-least-once delivery**. Your endpoint may receive the same event more than once. Your handler must be designed for this, or data corruption will follow — silently, without exceptions.

This repository documents the patterns that address this and the surrounding reliability concerns: idempotency, asynchronous processing, retry with backoff, dead letter queues, and replay tooling.

---

## Problem Statement

A webhook handler written for the happy path looks like this:

```python
@app.post("/webhooks/orders/create")
async def handle_order_created(request: Request):
    payload = await request.json()
    await push_order_to_erp(payload)
    return {"ok": True}
```

This fails in production in predictable ways:

- If `push_order_to_erp` takes longer than Shopify's delivery timeout window, Shopify retries. The same order lands in the ERP twice.
- If your service restarts during processing, Shopify retries. The order is processed again on restart.
- If the ERP API is temporarily unavailable, the handler returns a non-2xx response, Shopify retries, and the ERP may or may not be available on retry — with no guarantee of eventual delivery from your side.
- If your handler processes the event synchronously, a slow ERP response blocks the acknowledgment, increasing the risk of timeout and retry.

None of these produce an exception. They produce wrong data.

---

## Why Shopify Webhook Reliability Matters

At low order volumes, a fragile webhook handler is invisible. At production scale — and especially during peak periods like BFCM — the failure modes compound:

- **Duplicate ERP records** — orders processed twice, inventory decremented twice, fulfillments triggered twice
- **Silent data loss** — events dropped when downstream systems are slow or unavailable
- **Cascading failures** — one slow downstream system causes webhook timeout and retry storms
- **Invisible backlog** — no visibility into which events succeeded, which failed, and which need attention

The patterns in this repository address each of these. They are not new patterns — they are standard distributed systems techniques applied to the specific constraints of the Shopify webhook delivery model.

---

## Shopify Webhook Delivery Model

Key properties to design around (verify against current Shopify documentation for exact specifications):

**At-least-once delivery.** Shopify will attempt to deliver each event at least once. Under certain conditions — timeout, non-2xx response, service restart — it will attempt delivery again. Your handler must treat duplicate delivery as a normal operating condition, not an edge case.

**Retry behavior.** When your endpoint does not return a 2xx response promptly, Shopify will retry delivery with increasing delays. The retry window extends over a period of hours. Verify the current retry schedule and maximum attempt count in the Shopify developer documentation before production deployment.

**No ordering guarantee.** Multiple webhook events from the same shop may arrive out of order. An `orders/updated` event may arrive before the corresponding `orders/create`. Your handler must not assume that events arrive in the sequence they were generated.

**Delivery timeout.** Shopify expects a response within a short window (verify the current timeout in Shopify documentation). Processing the event synchronously in the handler risks exceeding this window and triggering a retry. Return 200 immediately; process asynchronously.

**HMAC signature.** Every webhook delivery includes an HMAC-SHA256 signature in the `X-Shopify-Hmac-Sha256` header. Verify this signature against the raw request body before processing. Reject any request that fails verification.

---

## Common Failure Modes

### 1. Synchronous processing exceeds delivery timeout
The handler calls the ERP, the ERP is slow, Shopify times out, Shopify retries. Now two workers are processing the same event.

### 2. No idempotency — duplicate creates duplicate records
The retry delivers the same `orders/create` event. The handler creates a second ERP order. No exception is raised. The duplicate is discovered manually.

### 3. ERP unavailability drops events silently
The ERP API returns 503. The handler returns 500. Shopify retries, the ERP is still unavailable, Shopify eventually stops retrying. The order is never pushed to the ERP. No alert fires.

### 4. No dead letter queue — failed events are unrecoverable
After retries are exhausted, failed events have nowhere to go. There is no payload to replay when the ERP recovers. The event is lost.

### 5. Missing observability — no visibility into handler health
Webhook processing lag, retry rates, dead letter queue depth, and ERP write success rates are not instrumented. Problems are discovered by merchants, not by monitoring.

---

## Architecture Overview

```
┌─────────────┐
│   Shopify   │
└──────┬──────┘
       │  POST /webhooks/{topic}
       ▼
┌─────────────────────────────────┐
│       Webhook Receiver          │
│       (FastAPI)                 │
│                                 │
│  1. Verify HMAC signature       │
│  2. Check idempotency           │
│  3. Persist event metadata      │
│  4. Enqueue processing job      │
│  5. Return HTTP 200 immediately │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       Event Store               │
│       (PostgreSQL)              │
│  - Raw payload                  │
│  - Webhook ID                   │
│  - Processing status            │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       Message Queue             │
│       (Redis Streams)           │
└────────────────┬────────────────┘
                 │ (async)
                 ▼
┌─────────────────────────────────┐
│       Worker Pool               │
│                                 │
│  - Process event                │
│  - Retry with exponential       │
│    backoff on failure           │
│  - Dead letter queue after      │
│    max attempts                 │
└──────┬─────────────┬────────────┘
       │             │
       ▼             ▼
┌──────────┐   ┌──────────────────┐
│   ERP    │   │  Dead Letter     │
│  WMS     │   │  Queue           │
│  CRM     │   │  (PostgreSQL)    │
└──────────┘   └────────┬─────────┘
                        │
                        ▼
               ┌────────────────┐
               │ Replay Endpoint│
               │ (admin-only)   │
               └────────────────┘
```

Mermaid source files:
- [diagrams/webhook-flow.mmd](diagrams/webhook-flow.mmd) — end-to-end delivery path
- [diagrams/async-processing-pipeline.mmd](diagrams/async-processing-pipeline.mmd) — receiver, queue, worker, target systems
- [diagrams/retry-lifecycle.mmd](diagrams/retry-lifecycle.mmd) — job state machine from received to DLQ
- [diagrams/dead-letter-replay.mmd](diagrams/dead-letter-replay.mmd) — DLQ review and replay flow

---

## Idempotency Strategies

The idempotency key is the `X-Shopify-Webhook-Id` header. This value is consistent across retry attempts for the same event — a retry of the same delivery carries the same webhook ID.

Three implementation patterns are documented in this repository:

### Pattern A: Redis SET NX (recommended for most cases)

Store the webhook ID in Redis with a TTL that exceeds the webhook retry window. Use `SET NX` (set if not exists) — this is atomic, which prevents race conditions between concurrent retries.

```python
acquired = await redis.set(
    f"webhook:processed:{webhook_id}",
    "1",
    nx=True,       # only set if key does not exist
    ex=172800      # 48-hour TTL — adjust based on Shopify's retry window
)
if not acquired:
    return  # already processed, skip
```

**Trade-offs:** Requires Redis. Keys expire automatically — no cleanup needed. Not durable across Redis failures unless persistence is configured.

### Pattern B: PostgreSQL unique constraint

Insert the webhook ID into a table with a unique constraint. Handle the `UniqueViolation` exception as a duplicate signal.

```sql
INSERT INTO processed_webhooks (webhook_id, processed_at)
VALUES ($1, NOW())
ON CONFLICT (webhook_id) DO NOTHING
RETURNING webhook_id;
```

If no row is returned, the event was already processed. **Trade-offs:** No additional infrastructure dependency. Requires periodic cleanup of old records. Slightly higher latency than Redis.

### Pattern C: Idempotency at the target system

Some ERP systems support an external reference ID on record creation. Passing the Shopify webhook ID or order ID as the external reference allows the ERP to reject duplicates natively.

**Trade-offs:** Requires ERP support. Shifts deduplication downstream — a failure between processing and ERP write can still produce a duplicate if the idempotency check passes before the ERP write is attempted.

See `docs/idempotency-patterns.md` and `examples/redis-idempotency/` and `examples/postgres-idempotency/` for full implementations.

---

## Async Processing Pattern

The webhook receiver must return HTTP 200 within Shopify's delivery timeout window. Any processing that depends on external system calls — ERP writes, WMS updates, database queries — must happen asynchronously.

The pattern:

1. Receive the request
2. Verify HMAC
3. Check idempotency
4. Persist the raw event payload to the event store
5. Enqueue a processing job
6. Return 200

The worker then:

1. Picks up the job from the queue
2. Retrieves the event payload from the store
3. Calls the target system
4. On success: marks the job complete
5. On failure: increments retry count, re-enqueues with backoff delay, or moves to dead letter queue

The receiver never calls the ERP. The worker never knows about Shopify's delivery model.

---

## Retry Strategy

Transient failures — ERP timeouts, brief unavailability, rate limit responses — should be retried. Permanent failures — schema mismatches, authorization errors, invalid payloads — should not.

**Exponential backoff:**

```
attempt 1: wait 2s
attempt 2: wait 4s
attempt 3: wait 8s
attempt 4: wait 16s
attempt 5: wait 32s
→ dead letter queue
```

Add jitter to the backoff to prevent thundering herd conditions when many retries fire simultaneously after a recovery:

```python
import random
delay = (2 ** attempt) + random.uniform(0, 1)
```

**Classify failures before retrying.** A 401 from the ERP API should not be retried — credentials have not changed between attempts. A 503 should be retried. Build a classifier that maps error types to retry/no-retry decisions.

See `docs/retry-strategy.md` and `examples/retry-worker/`.

---

## Dead Letter Queue Design

When a job exhausts its retry attempts, it moves to the dead letter queue. The DLQ is not a discard bin — it is a structured store for events that require human review and eventual replay.

The DLQ entry should contain:

- Original webhook payload (complete, not summarized)
- Webhook ID and topic
- Timestamp of first delivery attempt
- Error history: each attempt, its error message, and its timestamp
- Current status: `pending_review`, `resolved`, `discarded`

Operations the DLQ must support:

- **List:** view DLQ entries filtered by topic, status, time range
- **Inspect:** view full payload and error history for a specific entry
- **Replay:** re-enqueue a specific entry for processing
- **Discard:** mark an entry as intentionally dropped, with a reason

The DLQ replay endpoint must be protected. It should not be accessible from the public internet — it is an internal operational tool.

See `docs/dead-letter-queues.md` and `examples/replay-endpoint/`.

---

## Replay Tooling

Replay is the recovery mechanism when events in the DLQ can be re-processed after a root cause is resolved. Common scenarios:

- ERP API was unavailable for 4 hours. Events queued in DLQ during the outage. ERP recovers. Replay all DLQ entries from the outage window.
- Schema mismatch caused a class of events to fail. Schema adapter is fixed. Replay affected events.
- Worker had a bug that caused incorrect processing. Bug is fixed. Replay events processed during the affected window.

**Before replaying:**

1. Confirm the root cause is resolved — replaying into an unfixed system produces the same failures
2. Identify the scope of affected events (by topic, time range, error message)
3. Replay a single event first to verify the fix works
4. Replay the full set

See `docs/replay-tooling.md` and `examples/replay-endpoint/`.

---

## Observability

A webhook handler without observability is a black box. You will not know it is failing until a merchant notices missing data. Instrument the following:

| Metric | Description | Alert condition |
|---|---|---|
| `webhook_receive_total` | Total webhooks received, by topic | baseline monitoring |
| `webhook_duplicate_total` | Webhooks rejected by idempotency check | >5% of total is unusual |
| `webhook_processing_duration_seconds` | Time from enqueue to successful processing | p99 > 30s is concerning |
| `webhook_retry_total` | Total retry attempts | growing trend indicates systemic issue |
| `dlq_size` | Current DLQ depth | any growth requires attention |
| `erp_write_success_rate` | ERP write success percentage | <95% sustained is a problem |
| `queue_depth` | Current job queue depth | growing consistently = backpressure |

Emit structured logs at every stage boundary:

```json
{
  "timestamp": "2026-01-15T14:32:10Z",
  "event": "webhook.received",
  "webhook_id": "abc-123",
  "topic": "orders/create",
  "shop_domain": "example.myshopify.com",
  "is_duplicate": false
}
```

See `docs/observability.md` for the full instrumentation guide.

---

## Testing Duplicate Deliveries

Production reliability testing requires simulating the conditions that trigger duplicates. Three approaches:

**1. Direct handler test (unit):**
Call the webhook handler twice with the same webhook ID. Assert that the second call returns 200 and produces no side effects.

**2. Queue-level test (integration):**
Enqueue the same job twice. Assert that only one write reaches the target system.

**3. Load test with forced retries:**
Use a test webhook endpoint that returns 5xx for the first N attempts. Verify that the idempotency layer prevents duplicate processing when Shopify retries and the handler eventually succeeds.

Do not test reliability only in the happy path. The happy path is not where the failure modes live.

---

## Design Trade-offs

### Why PostgreSQL for the event store instead of Redis only?

Redis is fast and appropriate for the idempotency key store. But for the event store — the persistent record of every event received — durability matters more than speed. PostgreSQL with WAL provides a durable audit trail. Redis without persistence configured is not appropriate for this.

### Why async processing instead of synchronous?

Synchronous processing couples the Shopify delivery timeout to the ERP response time. If the ERP is slow, the handler times out, Shopify retries, and you have a duplicate processing problem. Async processing decouples these — the receiver is always fast, the worker processes at whatever rate the ERP can handle.

### Why a separate dead letter queue instead of re-queuing indefinitely?

Indefinite re-queuing obscures the queue health and can produce unbounded growth. Moving exhausted failures to a DLQ makes the failure visible, preserves the payload, and separates "normal processing" from "items requiring attention." It also prevents a poison pill event from blocking the queue indefinitely.

### Why not use a managed queue service?

This reference uses Redis Streams, which requires operating a Redis instance. Managed queue services (AWS SQS, Google Pub/Sub, etc.) are operationally simpler and appropriate for production. The patterns documented here apply regardless of the underlying queue technology. Redis is used here because it is already a common infrastructure component in Shopify integration stacks (session storage, rate limiting, caching).

---

## Production Considerations

Before deploying a webhook integration to production:

- [ ] HMAC verification is enabled and tested with an invalid signature
- [ ] Idempotency is implemented and tested with duplicate delivery simulation
- [ ] Event store is durable (PostgreSQL with backups, not in-memory)
- [ ] Processing is asynchronous — receiver returns 200 before any ERP calls
- [ ] Retry strategy classifies failures correctly (retriable vs. non-retriable)
- [ ] Dead letter queue is operational and has a documented review process
- [ ] Replay endpoint is protected (not accessible from public internet)
- [ ] All stage boundaries emit structured logs
- [ ] Key metrics are instrumented and alerts are configured
- [ ] DLQ alert fires when depth exceeds zero
- [ ] Queue depth alert fires on sustained growth
- [ ] Worker pool has enough concurrency for peak event volume
- [ ] ERP rate limits are known (documented or empirically measured) and respected

See `docs/production-considerations.md` for the full checklist.

---

## Example Services

| Example | Description |
|---|---|
| `examples/fastapi-webhook-receiver/` | Complete webhook receiver with HMAC verification, Redis idempotency, event persistence, and queue enqueue |
| `examples/redis-idempotency/` | Redis SET NX idempotency pattern with TTL management and failure handling |
| `examples/postgres-idempotency/` | PostgreSQL unique constraint idempotency pattern with cleanup job |
| `examples/retry-worker/` | Async worker with exponential backoff, failure classification, and dead letter queue |
| `examples/replay-endpoint/` | Admin endpoint for DLQ inspection, replay, and discard operations |

Each example includes inline comments explaining the design decision behind each implementation choice.

---

## Running Locally

**Prerequisites:** Docker and Docker Compose.

```bash
cp .env.example .env
# Edit .env — set SHOPIFY_WEBHOOK_SECRET to any test value for local development

docker-compose up -d
```

Services started:
- `receiver` — FastAPI webhook receiver on port 8000
- `worker` — async processing worker
- `postgres` — event store and DLQ
- `redis` — idempotency store and message queue

Test webhook delivery:

```bash
# Generate a test HMAC for local development
python -c "
import hmac, hashlib, base64
body = b'{\"id\": 1, \"test\": true}'
secret = b'your-test-secret'
sig = base64.b64encode(hmac.new(secret, body, hashlib.sha256).digest()).decode()
print(sig)
"

curl -X POST http://localhost:8000/webhooks/orders/create \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Webhook-Id: test-webhook-001" \
  -H "X-Shopify-Hmac-Sha256: <generated_signature>" \
  -H "X-Shopify-Topic: orders/create" \
  -H "X-Shopify-Shop-Domain: example.myshopify.com" \
  -d '{"id": 1, "test": true}'
```

---

## Related Repositories

**[shopify-integration-architecture](https://github.com/LeoCodeIt/shopify-integration-architecture)**
The broader integration architecture this repository extends — middleware design, event flow, ERP adapter patterns, and observability for the full integration stack.

**Future reading:**
Codepunklab articles on Shopify Plus integration architecture: [codepunklab.com](https://codepunklab.com)

---

## Roadmap

- [ ] Add OpenTelemetry tracing across receiver → queue → worker
- [ ] Add Prometheus metrics exporter with Grafana dashboard definition
- [ ] Add example ERP adapter implementation (generic REST target)
- [ ] Add webhook topic routing (single receiver, multiple handlers)
- [ ] Document multi-shop webhook handling patterns
- [ ] Add load test scripts for duplicate delivery simulation

---

## Author

Maintained by [Leonardo Pedani](https://github.com/LeoCodeIt).

[Codepunklab](https://codepunklab.com) is an independent engineering lab focused on scalable ecommerce systems, middleware architecture, and Shopify Plus integration patterns.

---

## License

MIT — see [LICENSE](LICENSE).

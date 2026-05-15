# FastAPI Webhook Receiver — Reference Implementation

A complete, runnable reference implementation of the webhook receiver pattern described in the main repository documentation. This is not a production starter — it is a reference to read alongside the architecture documentation.

---

## What This Implements

- HMAC-SHA256 signature verification using timing-safe comparison
- Atomic idempotency check via Redis SET NX
- Event persistence to PostgreSQL before enqueueing
- Job enqueueing to Redis Streams
- Prometheus metrics at `/metrics`
- Structured JSON logging via structlog
- Health check endpoint at `/health`
- Automatic `webhook_events` table creation on startup

---

## Running Locally

**Prerequisites:** Docker and Docker Compose (see `docker-compose.yml` at the repository root).

```bash
# From the repository root
cp .env.example .env
# Edit .env with your SHOPIFY_WEBHOOK_SECRET

docker compose up postgres redis

# In a separate terminal, from this directory
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Or run everything via Docker Compose:

```bash
docker compose up
```

---

## Testing the Receiver

Generate a valid HMAC signature for a test payload:

```python
import hashlib
import hmac
import base64
import json

secret = "your-webhook-secret"
payload = json.dumps({"id": 12345, "test": True}).encode()

signature = base64.b64encode(
    hmac.new(secret.encode(), payload, hashlib.sha256).digest()
).decode()

print(signature)
```

Send the webhook:

```bash
curl -X POST http://localhost:8000/webhooks/orders/create \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Hmac-Sha256: <signature>" \
  -H "X-Shopify-Webhook-Id: test-webhook-001" \
  -H "X-Shopify-Shop-Domain: example.myshopify.com" \
  -H "X-Shopify-Topic: orders/create" \
  -d '{"id": 12345, "test": true}'
```

Expected response:

```json
{"status": "accepted", "event_id": 1}
```

Send the same request again (duplicate):

```json
{"status": "duplicate"}
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SHOPIFY_WEBHOOK_SECRET` | Yes | — | Webhook HMAC signing secret from Shopify Partners |
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `REDIS_URL` | Yes | — | Redis connection string |
| `IDEMPOTENCY_TTL_SECONDS` | No | `172800` | How long to retain idempotency keys (48 hours) |
| `LOG_LEVEL` | No | `INFO` | Log level |
| `ENVIRONMENT` | No | `development` | Environment name included in startup log |

---

## Key Implementation Decisions

**Raw body before parsing.** The HMAC is computed over the raw request bytes, not the parsed JSON. The body is read once with `await request.body()` before any JSON parsing.

**Idempotency key release on failure.** If the Redis SET NX succeeds but the subsequent persist or enqueue fails, the key is deleted. This allows the next delivery attempt to be treated as a new event rather than a duplicate. If the delete also fails (Redis unavailable), the next attempt will be treated as a duplicate and silently skipped — acceptable, because the alternative (stuck in failed state permanently) is worse.

**Persist before enqueue.** The event is written to PostgreSQL before the Redis Streams XADD. If enqueue fails after persist succeeds, the event exists in the database but has no worker job. This is detectable and recoverable — query for events in `queued` status with no corresponding stream entry.

**Return 200 for duplicates.** Shopify expects a 2xx response. A 409 or other error code on a duplicate would cause Shopify to retry, generating more duplicates.

**Return 500 on enqueue failure.** If persist or enqueue fails, returning 500 causes Shopify to retry delivery. Combined with idempotency key release, the next retry is treated as a new event and processed correctly.

---

## What This Does Not Include

- Worker implementation (see `examples/retry-worker/`)
- DLQ management (see `examples/replay-endpoint/`)
- Multi-shop webhook secret routing
- Authentication middleware for admin endpoints
- Database migration tooling (Alembic, Flyway)
- TLS termination (handled at infrastructure level)

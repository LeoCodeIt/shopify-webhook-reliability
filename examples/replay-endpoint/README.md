# Replay Endpoint — Reference Implementation

An authenticated FastAPI service for inspecting the dead letter queue and re-enqueuing events for processing. This is an administrative tool — it must not be publicly accessible.

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/dlq` | List DLQ entries by status, topic |
| `GET` | `/dlq/{id}` | Get single entry with full error history |
| `POST` | `/dlq/replay` | Re-enqueue a single entry |
| `POST` | `/dlq/replay/bulk` | Re-enqueue by topic and time window (dry_run supported) |
| `POST` | `/dlq/replay/force` | Clear idempotency key and re-enqueue |
| `POST` | `/dlq/discard` | Mark entry as discarded (requires reason) |
| `GET` | `/health` | Redis and PostgreSQL connectivity check |

All endpoints except `/health` require `X-Replay-Api-Key` header.

---

## Authentication

Set the API key in your `.env`:

```
REPLAY_API_KEY=your-secret-key
```

Include it in every request:

```bash
curl -H "X-Replay-Api-Key: your-secret-key" http://localhost:8001/dlq
```

API key alone is insufficient for production. Deploy behind a VPN or private network boundary, and restrict access at the infrastructure level.

---

## Typical Replay Workflow

**1. Inspect — understand what failed and why**

```bash
# List all pending entries for a topic
curl -H "X-Replay-Api-Key: ..." \
  "http://localhost:8001/dlq?topic=orders/create&status=pending_review"

# Get full error history for a specific entry
curl -H "X-Replay-Api-Key: ..." \
  "http://localhost:8001/dlq/550e8400-e29b-41d4-a716-446655440000"
```

**2. Dry run — verify scope before committing**

```bash
curl -X POST -H "X-Replay-Api-Key: ..." \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders/create",
    "since": "2026-01-15T14:00:00Z",
    "until": "2026-01-15T18:00:00Z",
    "dry_run": true
  }' http://localhost:8001/dlq/replay/bulk
```

**3. Replay — re-enqueue the entries**

```bash
# Same request with dry_run: false
curl -X POST -H "X-Replay-Api-Key: ..." \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders/create",
    "since": "2026-01-15T14:00:00Z",
    "until": "2026-01-15T18:00:00Z",
    "dry_run": false
  }' http://localhost:8001/dlq/replay/bulk
```

**4. Verify — check that replayed entries resolved**

```bash
# Check replaying entries (should move to resolved as worker processes them)
curl -H "X-Replay-Api-Key: ..." \
  "http://localhost:8001/dlq?status=replaying"

# Check for new DLQ entries (failed replay batch produces new entries)
curl -H "X-Replay-Api-Key: ..." \
  "http://localhost:8001/dlq?status=pending_review"
```

---

## Force Replay

If the idempotency key is still valid and preventing replay:

```bash
curl -X POST -H "X-Replay-Api-Key: ..." \
  -H "Content-Type: application/json" \
  -d '{
    "dlq_id": "550e8400-e29b-41d4-a716-446655440000",
    "webhook_id": "abc123",
    "reason": "ERP confirmed write never reached DB — safe to reprocess"
  }' http://localhost:8001/dlq/replay/force
```

Force replay overrides the idempotency check. Document every use.

---

## Running

```bash
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/webhooks"
export REDIS_URL="redis://localhost:6379"
export REPLAY_API_KEY="your-secret-key"

uvicorn replay:app --port 8001
```

Do not expose port 8001 publicly. Bind to localhost or a private interface only.

---

## What This Does Not Include

- Network-level access control (VPN, IP allowlist) — configure at infrastructure level
- Audit log beyond structlog JSON output
- Rate limiting on the replay endpoint
- Pagination for large DLQ result sets (use `limit` parameter)

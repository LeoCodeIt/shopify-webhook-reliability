# Redis Idempotency — Reference Implementation

Demonstrates the three Redis-based idempotency patterns described in `docs/idempotency-patterns.md`. Read alongside the documentation to understand the design decisions behind each pattern.

---

## Patterns

### 1. `check_and_mark` — atomic SET NX

The core pattern used in the receiver. A single Redis command atomically checks whether the key exists and sets it if not.

```python
is_dup = await check_and_mark(r, webhook_id)
if is_dup:
    return "already processed"
# process the event
```

**Why SET NX instead of GET then SET:** a GET/SET sequence has a race window. Two concurrent requests can both GET and see "key does not exist", then both SET and both proceed to process — defeating idempotency. SET NX collapses check and set into one atomic operation.

### 2. `process_with_idempotency` — SET NX with delete-on-failure

Wraps process_fn with idempotency: marks the key before calling process_fn, releases the key if process_fn raises.

```python
result = await process_with_idempotency(
    r, webhook_id, lambda: process_event(payload)
)
# result: "processed" | "duplicate" | "failed"
```

If process_fn fails and the key is released, the next delivery attempt is treated as a new event. This is correct — a failed event should be retried, not silently dropped.

### 3. `inspect_keys` — batch TTL inspection

Read-only inspection of multiple idempotency keys. Used for debugging and DLQ replay planning.

```python
statuses = await inspect_keys(r, ["wh-001", "wh-002", "wh-003"])
for s in statuses:
    print(s.webhook_id, s.is_marked, s.ttl_seconds)
```

---

## TTL Sizing

The default TTL is 172800 seconds (48 hours). This value should exceed Shopify's delivery retry window to ensure duplicates are caught across all retry attempts.

Verify the current Shopify retry window in Shopify's documentation before adjusting this value. The retry window is an operational detail that Shopify may update.

---

## Force Clear for Replay

`clear_for_replay` explicitly deletes an idempotency key to allow re-processing. Use this only when you are certain the event needs to be reprocessed — for example, when a silent processing failure occurred after the key was marked.

```python
await clear_for_replay(r, webhook_id, reason="ERP write failed silently — confirmed in ERP audit log")
```

Document every use. This overrides a safety mechanism.

---

## Running the Demo

```bash
pip install -r requirements.txt

# Redis must be running
redis-server

python redis_idempotency.py
```

Expected output:

```
Webhook ID: demo-1720000000
First call — is_duplicate: False
Second call — is_duplicate: True
  demo-1720000000: marked=True, ttl=59s
  nonexistent-id: marked=False, ttl=None
After clear — is_duplicate: False
```

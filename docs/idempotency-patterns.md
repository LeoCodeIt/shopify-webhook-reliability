# Idempotency Patterns

Idempotency means that processing the same event multiple times produces the same outcome as processing it once. For webhook handlers, this is a correctness requirement — not a performance optimization.

The idempotency key for Shopify webhooks is the `X-Shopify-Webhook-Id` header. This value is stable across retry attempts for the same event delivery.

---

## Pattern A: Redis SET NX

Store the webhook ID in Redis using `SET NX` (set if not exists). This is atomic — if two concurrent requests with the same webhook ID arrive simultaneously, exactly one will acquire the key. The other will see the key already exists and exit.

```python
import redis.asyncio as redis

IDEMPOTENCY_TTL = 172800  # 48 hours in seconds
                           # Should exceed the webhook retry window.
                           # Verify current Shopify retry window in documentation.

async def is_already_processed(r: redis.Redis, webhook_id: str) -> bool:
    key = f"webhook:processed:{webhook_id}"
    # SET NX EX: set only if not exists, with TTL
    # Returns True if key was set (new event), False if key already existed (duplicate)
    acquired = await r.set(key, "1", nx=True, ex=IDEMPOTENCY_TTL)
    return not acquired  # True = already processed, False = new event

async def handle_webhook(webhook_id: str, payload: dict, r: redis.Redis):
    if await is_already_processed(r, webhook_id):
        return  # duplicate — exit cleanly, return 200 to Shopify

    try:
        await process_event(payload)
    except Exception:
        # Processing failed — delete the key so retry can be attempted
        # Trade-off: if deletion fails and Shopify retries, the retry will be
        # treated as a duplicate and silently skipped. Prefer this over
        # leaving a failed event unprocessed.
        await r.delete(f"webhook:processed:{webhook_id}")
        raise
```

**Why delete the key on failure?**
If processing fails and you keep the key, Shopify's retry will be ignored — the event is stuck in a failed state with no recovery path. Deleting the key allows the retry to be treated as a new event. The trade-off: if deletion fails (Redis is unavailable), the retry will be silently skipped. This is an edge case within an edge case, but document it explicitly.

**TTL rationale:** The TTL should exceed Shopify's retry window so that a late retry does not slip through after the key expires. If the key expires and Shopify delivers a late retry, the event will be processed again. Setting the TTL significantly longer than the retry window is cheap (Redis memory) and prevents this edge case.

**When to choose Redis idempotency:**
- Redis is already in the infrastructure (caching, sessions)
- Low latency is important for the idempotency check
- Redis persistence is configured (AOF or RDB)
- The idempotency data does not need to survive Redis restarts with certainty

---

## Pattern B: PostgreSQL Unique Constraint

Store processed webhook IDs in a database table with a unique constraint. Attempt an insert — if the insert succeeds, the event is new. If it raises a unique constraint violation, the event is a duplicate.

```sql
-- schema.sql
CREATE TABLE processed_webhooks (
    webhook_id  TEXT PRIMARY KEY,
    topic       TEXT NOT NULL,
    shop_domain TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

```python
import asyncpg

async def check_and_record_idempotency(
    conn: asyncpg.Connection,
    webhook_id: str,
    topic: str,
    shop_domain: str
) -> bool:
    """
    Returns True if this webhook_id is new and was recorded.
    Returns False if it already exists (duplicate).

    Uses INSERT ... ON CONFLICT DO NOTHING to handle race conditions atomically.
    """
    result = await conn.fetchrow(
        """
        INSERT INTO processed_webhooks (webhook_id, topic, shop_domain)
        VALUES ($1, $2, $3)
        ON CONFLICT (webhook_id) DO NOTHING
        RETURNING webhook_id
        """,
        webhook_id, topic, shop_domain
    )
    return result is not None  # None = conflict, record already existed
```

**Cleanup job:** unlike Redis, PostgreSQL does not expire keys automatically. A cleanup job must remove old records. Entries older than the webhook retry window can be safely deleted.

```python
async def cleanup_old_idempotency_records(conn: asyncpg.Connection, retention_days: int = 3):
    """Run this periodically — daily is sufficient."""
    deleted = await conn.execute(
        "DELETE FROM processed_webhooks WHERE processed_at < NOW() - $1::interval",
        f"{retention_days} days"
    )
    return deleted
```

**When to choose PostgreSQL idempotency:**
- No Redis in the infrastructure
- Idempotency records need to survive infrastructure restarts with certainty
- Compliance or audit requirements mandate durable records of all processed events
- Slightly higher latency is acceptable

---

## Pattern C: Idempotency at the Target System

Some target systems support an external reference ID on record creation. If the ERP accepts an `external_id` field and enforces uniqueness on it, passing the Shopify order ID (or webhook ID) as the external reference allows the ERP to reject duplicates natively.

```python
async def push_order_to_erp(order: dict) -> dict:
    payload = {
        "external_id": f"shopify-{order['id']}",  # ERP enforces uniqueness on this field
        "order_number": order["order_number"],
        "line_items": map_line_items(order["line_items"]),
        # ...
    }
    response = await erp_client.post("/orders", json=payload)

    if response.status_code == 409:
        # ERP rejected as duplicate — treat as success (already processed)
        return {"status": "duplicate", "external_id": payload["external_id"]}

    response.raise_for_status()
    return response.json()
```

**When to choose target-system idempotency:**
- The target system explicitly supports and enforces external IDs
- Simplicity is a priority (no additional infrastructure for idempotency)
- The ERP's idempotency behavior is well-documented and tested

**Limitations:**
- Not all ERPs support external IDs or enforce uniqueness on them
- A failure between your idempotency check and the ERP write can still produce a duplicate — the ERP rejects it, but your handler sees a 409 and must handle it correctly
- Deduplication logic is distributed between your system and the ERP — harder to observe and debug

---

## Choosing a Pattern

| Criterion | Redis NX | PostgreSQL | Target System |
|---|---|---|---|
| Latency | Lowest (~1ms) | Low (~5ms) | Depends on ERP |
| Durability | Depends on Redis config | High | High |
| Infrastructure requirement | Redis | None (if PostgreSQL already used) | ERP support |
| Auto-cleanup | Yes (TTL) | No (cleanup job needed) | Varies |
| Observability | Key count via Redis CLI | Query table | ERP API |
| Race condition safe | Yes (SET NX is atomic) | Yes (ON CONFLICT) | Depends on ERP |

For most Shopify Plus integration architectures, **Pattern A (Redis NX)** is the default choice — it is fast, atomic, and Redis is typically already in the stack. Use **Pattern B (PostgreSQL)** when durability guarantees are critical. Use **Pattern C** only when the ERP's idempotency support is verified and well-understood.

---

## What Idempotency Does Not Solve

Idempotency prevents duplicate processing of the same webhook event. It does not prevent:

- **Out-of-order processing** — events processed in the wrong sequence can still produce incorrect state. Handle with timestamp-based logic in the processor.
- **Partial processing** — if processing succeeds partially (3 of 5 line items written to ERP), idempotency will not retry the failed items. Handle with atomic processing or compensating transactions.
- **Data corruption from incorrect business logic** — idempotency guarantees the handler runs once. If the handler has a bug, it will run incorrectly once.

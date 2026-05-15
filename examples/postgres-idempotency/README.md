# PostgreSQL Idempotency — Reference Implementation

Demonstrates the PostgreSQL unique constraint idempotency pattern from `docs/idempotency-patterns.md`. Use this when durability requirements exceed what Redis persistence can guarantee, or when you prefer to keep the idempotency store co-located with the event store.

---

## How It Works

A `processed_webhooks` table with a unique index on `webhook_id`. Marking an event as received is an INSERT. If the INSERT raises `UniqueViolationError`, the event is a duplicate.

```python
is_new = await mark_received(conn, webhook_id, topic, shop_domain)
if not is_new:
    return "duplicate"
# process the event
```

The unique index ensures this is atomic at the database level. Concurrent inserts with the same webhook_id serialize, and only one will succeed.

---

## Comparison With Redis SET NX

| | Redis SET NX | PostgreSQL Unique |
|---|---|---|
| Atomicity | Single command | Unique index constraint |
| Latency | ~1ms (in-memory) | ~5-20ms (disk write) |
| Durability | Configurable (AOF/RDB) | WAL-guaranteed |
| Expiry | Automatic TTL | Manual cleanup required |
| Survives restart | With AOF persistence | Always |
| Query flexibility | Limited | Full SQL |

---

## Retention and Cleanup

PostgreSQL records do not expire automatically. Run `cleanup_expired()` periodically to prevent unbounded table growth:

```python
deleted = await cleanup_expired(conn, retention_hours=72)
```

The retention window must exceed Shopify's delivery retry window. 72 hours (3 days) provides comfortable margin above 48 hours. Verify the current retry window in Shopify's documentation before setting a shorter retention period.

Use `pg_cron` or a scheduled job (cron, Kubernetes CronJob) to run cleanup daily.

---

## Failure Handling

Unlike Redis (where the key is deleted on failure), PostgreSQL uses an `outcome` column:

- `processed`: event was successfully handled
- `failed`: event was received but processing failed
- `skipped`: event was deduplicated

If you want future retries to be treated as new events, call `delete_for_retry()` to remove the record. If you want to track the failure without allowing retry (because retry is handled by the job queue), call `mark_failed()` and let the worker manage retries.

---

## Running the Demo

```bash
pip install -r requirements.txt

# PostgreSQL must be running with a 'webhooks' database
createdb webhooks

python postgres_idempotency.py
```

Expected output:

```
Webhook ID: demo-pg-1720000000
First INSERT — is_new: True
Second INSERT — is_new: False
Status: {'webhook_id': 'demo-pg-...', 'topic': 'orders/create', 'outcome': 'processed', ...}
After mark_failed — outcome: failed
Deleted for retry: True
After delete — is_new: True
```

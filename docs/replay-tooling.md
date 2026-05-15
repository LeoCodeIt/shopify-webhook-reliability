# Replay Tooling

Replay is the mechanism for re-processing events from the dead letter queue after a root cause has been resolved. It is a recovery operation — not a debugging tool, not a way to test changes in production.

---

## When to Replay

Replay is appropriate when:

1. **The root cause is resolved.** The ERP API is back online. The schema adapter bug is fixed and deployed. The rate limit issue is understood and mitigated. Replaying before the root cause is fixed produces the same failures.

2. **The scope is identified.** You know which events failed, when they failed, and why. Broad replays without scope identification can re-process events that did not actually fail, potentially causing duplicates.

3. **The fix is verified.** Replay one event manually and confirm it processes successfully before replaying a batch. A failed batch replay adds noise to the DLQ without recovering any data.

---

## Replay Scope Definition

Before initiating a replay, define the scope precisely:

```python
# Scope by topic and time window
scope = {
    "topic": "orders/create",
    "exhausted_after": datetime(2026, 1, 15, 14, 0, 0, tzinfo=timezone.utc),
    "exhausted_before": datetime(2026, 1, 15, 18, 0, 0, tzinfo=timezone.utc),
    "last_error_contains": "connection timeout"  # optional: filter by error type
}
```

A scope that is too broad risks re-processing events that succeeded (if the idempotency check has expired) or creating unnecessary noise. A scope that matches the actual failure window and error type is the correct approach.

---

## Replay Process

The replay endpoint re-enqueues DLQ entries to the main processing queue. The worker processes them using the current code — including any fixes that were deployed after the original failure.

**Critical:** the replay uses the original raw payload, not a transformed version. Schema adapters are applied at processing time, not at replay enqueue time. This means that if the schema adapter was the source of the bug, deploying a fixed adapter and replaying will correctly process the original payload.

```python
async def replay_by_scope(
    conn: asyncpg.Connection,
    queue: redis.Redis,
    topic: str,
    since: datetime,
    until: datetime,
    dry_run: bool = True  # default to dry_run — list without replaying
) -> dict:
    """
    Replay DLQ entries matching the scope.

    dry_run=True: return the list of entries that would be replayed, without re-enqueuing
    dry_run=False: re-enqueue entries and mark them as 'replaying'
    """
    entries = await conn.fetch(
        """
        SELECT id, webhook_id, topic, raw_payload
        FROM dead_letter_queue
        WHERE topic = $1
          AND exhausted_at BETWEEN $2 AND $3
          AND status = 'pending_review'
        ORDER BY exhausted_at ASC
        """,
        topic, since, until
    )

    if dry_run:
        return {"would_replay": len(entries), "entries": [dict(e) for e in entries]}

    replayed = 0
    failed = 0
    for entry in entries:
        success = await replay_dlq_entry(conn, queue, str(entry["id"]))
        if success:
            replayed += 1
        else:
            failed += 1

    return {"replayed": replayed, "failed_to_enqueue": failed}
```

---

## Replay Endpoint Security

The replay endpoint must not be accessible from the public internet. It is an administrative operation that can:

- Re-process events that have already been processed (if idempotency keys have expired)
- Generate significant load on the target system (batch replay of thousands of events)
- Be used to force arbitrary payloads through the processing pipeline

Protect it with a pre-shared API key at minimum. In production, it should be behind a VPN or private network boundary.

```python
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-Replay-Api-Key")

async def require_replay_key(api_key: str = Security(api_key_header)):
    if api_key != settings.REPLAY_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid replay API key")
    return api_key
```

---

## Idempotency During Replay

When an event is replayed, the idempotency check uses the original `webhook_id`. If the idempotency TTL has not expired, the replay will be treated as a duplicate and skipped.

This is the correct behavior for most cases — if the event was successfully processed before it moved to the DLQ (which should not happen, but could in edge cases), the replay should not re-process it.

If you need to force a replay of an event whose idempotency key is still valid (e.g., to correct a processing error where the event was marked processed but the ERP write failed silently), you must explicitly delete the idempotency key before replaying:

```python
async def force_replay(
    conn: asyncpg.Connection,
    queue: redis.Redis,
    r: redis.Redis,
    dlq_id: str,
    webhook_id: str
):
    # Delete idempotency key to allow re-processing
    # Only use this when you are certain the event needs to be reprocessed
    await r.delete(f"webhook:processed:{webhook_id}")
    await replay_dlq_entry(conn, queue, dlq_id)
```

Document every forced replay. It is an intentional override of a safety mechanism.

---

## Post-Replay Verification

After replaying, verify that the events processed successfully:

1. Check the DLQ entries moved from `replaying` to `resolved`
2. Check the target system for the expected records (spot-check a sample of replayed events)
3. Monitor the DLQ for new entries from the replay — a failed replay batch may produce new DLQ entries

A replay that results in new DLQ entries of the same type as the original failure indicates the root cause is not fully resolved.

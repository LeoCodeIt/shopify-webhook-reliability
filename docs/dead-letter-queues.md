# Dead Letter Queue Design

The dead letter queue (DLQ) is a structured store for webhook processing jobs that have exhausted their retry attempts. It is not a discard bin — it is a recovery mechanism. Every event in the DLQ represents data that has not reached its destination and will not reach it without intervention.

---

## Why a DLQ Is Required

Without a DLQ, exhausted retry jobs have two destinations: silent deletion or indefinite re-queuing. Neither is acceptable.

**Silent deletion** produces invisible data loss. The order never reached the ERP. No alert fired. No record exists. The merchant discovers the missing order when they look for it manually.

**Indefinite re-queuing** creates a poison pill: one event that always fails blocks the queue or consumes worker resources indefinitely. It also obscures queue health — a queue with 50 normal jobs and one job on its 200th retry looks very different from the outside depending on how retry state is represented.

The DLQ makes failure **visible**, **recoverable**, and **bounded**.

---

## What the DLQ Stores

Each DLQ entry must preserve enough context to diagnose the failure and replay the event after the root cause is resolved.

```sql
CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id      TEXT NOT NULL,
    topic           TEXT NOT NULL,
    shop_domain     TEXT NOT NULL,
    raw_payload     JSONB NOT NULL,       -- full original payload, never summarized
    enqueued_at     TIMESTAMPTZ NOT NULL, -- when the original event was first received
    exhausted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- when it moved to DLQ
    attempt_count   INT NOT NULL,
    error_history   JSONB NOT NULL,       -- array of {attempt, error, timestamp}
    last_error      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending_review',
                                          -- pending_review | replaying | resolved | discarded
    resolution_note TEXT,
    resolved_at     TIMESTAMPTZ,
    CONSTRAINT valid_status CHECK (status IN ('pending_review', 'replaying', 'resolved', 'discarded'))
);

CREATE INDEX dlq_status_idx ON dead_letter_queue (status);
CREATE INDEX dlq_topic_idx ON dead_letter_queue (topic);
CREATE INDEX dlq_exhausted_at_idx ON dead_letter_queue (exhausted_at);
```

**Raw payload is non-negotiable.** Do not store a summarized or transformed version of the payload. Store the original bytes that were received from Shopify. Schema adapters may have changed between the time of failure and the time of replay — the replay should apply the current adapter to the original payload, not a pre-transformed version.

**Error history over last-error-only.** The sequence of errors often tells you more than the final error. A job that fails with 503 for four attempts and then 400 on the fifth indicates the ERP recovered but the payload is now malformed — a different root cause than if all attempts returned 503.

---

## DLQ Operations

### Inspection

List DLQ entries filtered by criteria:

```python
async def list_dlq_entries(
    conn: asyncpg.Connection,
    status: str = "pending_review",
    topic: str | None = None,
    since: datetime | None = None,
    limit: int = 50
) -> list[dict]:
    query = """
        SELECT id, webhook_id, topic, shop_domain, last_error, attempt_count,
               exhausted_at, status
        FROM dead_letter_queue
        WHERE status = $1
          AND ($2::text IS NULL OR topic = $2)
          AND ($3::timestamptz IS NULL OR exhausted_at >= $3)
        ORDER BY exhausted_at DESC
        LIMIT $4
    """
    rows = await conn.fetch(query, status, topic, since, limit)
    return [dict(row) for row in rows]
```

### Replay

Re-enqueue a DLQ entry for processing. Mark it as `replaying` to prevent concurrent replay attempts.

```python
async def replay_dlq_entry(
    conn: asyncpg.Connection,
    queue: redis.Redis,
    dlq_id: str
) -> bool:
    # Mark as replaying — prevents duplicate replay
    result = await conn.fetchrow(
        """
        UPDATE dead_letter_queue
        SET status = 'replaying', resolved_at = NOW()
        WHERE id = $1 AND status = 'pending_review'
        RETURNING id, raw_payload, topic, webhook_id
        """,
        dlq_id
    )
    if not result:
        return False  # already being replayed or not found

    # Re-enqueue to the main processing queue
    # The worker will process it like a new event, applying current adapter logic
    await queue.xadd(
        "webhook_jobs",
        {
            "webhook_id": result["webhook_id"],
            "topic": result["topic"],
            "payload": result["raw_payload"],
            "source": "dlq_replay",
            "dlq_id": dlq_id
        }
    )
    return True
```

### Discard

Mark an entry as intentionally discarded. Require a reason — this is an audit record.

```python
async def discard_dlq_entry(
    conn: asyncpg.Connection,
    dlq_id: str,
    reason: str
) -> bool:
    result = await conn.execute(
        """
        UPDATE dead_letter_queue
        SET status = 'discarded',
            resolution_note = $2,
            resolved_at = NOW()
        WHERE id = $1 AND status = 'pending_review'
        """,
        dlq_id, reason
    )
    return result != "UPDATE 0"
```

---

## Alerting

The DLQ should alert on any growth. An empty DLQ is the expected steady state. A non-empty DLQ means events have not reached their destination.

Alert conditions:

- **Any new DLQ entry** — fire an alert when `exhausted_at` is set for a new row. This is the primary signal.
- **DLQ depth > N** — fire when total `pending_review` entries exceeds a threshold (e.g., 10). This indicates a systemic issue, not a one-off failure.
- **DLQ entries older than T hours** — fire when `pending_review` entries have been sitting unreviewed for more than a configured threshold (e.g., 24 hours). This catches cases where the alert fired but was ignored.

The replay and discard operations should silence the "entries older than T hours" alert, not the "new entry" alert. Every new DLQ entry should produce a notification regardless of DLQ history.

---

## DLQ Review Process

Define a documented process for DLQ review. An undocumented DLQ will accumulate entries that nobody reviews.

Recommended process:

1. Alert fires when a new entry lands in the DLQ
2. On-call engineer inspects the entry: `webhook_id`, `topic`, `error_history`
3. Diagnose root cause: ERP issue? Schema change? Rate limit? Bug?
4. If root cause is resolved: replay the entry and verify success
5. If root cause requires a code change: fix, deploy, then replay
6. If the entry is genuinely unrecoverable (e.g., a test event, or a known Shopify bug): discard with a descriptive reason
7. Close the alert

The process should be documented in your runbook, not just in this repository.

---

## DLQ vs. Shopify Retry

The DLQ on your side is separate from Shopify's retry mechanism. They operate at different layers:

- **Shopify's retry:** re-delivers the webhook to your endpoint when your endpoint returns non-2xx. This is about delivery, not processing.
- **Your DLQ:** stores jobs that were successfully received (your endpoint returned 200) but failed to process (the ERP write failed after all retries).

Once your endpoint returns 200, Shopify considers the delivery successful. What happens to the event after that — successful processing, failed processing, DLQ — is entirely within your system.

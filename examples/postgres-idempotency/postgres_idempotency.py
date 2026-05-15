"""
PostgreSQL-based idempotency for webhook processing.

Uses a unique constraint on webhook_id. A second INSERT raises
asyncpg.UniqueViolationError, which we catch and treat as a duplicate.

This is the durable alternative to Redis SET NX:
- Survives Redis restarts and failures
- Higher latency than in-memory Redis
- Requires periodic cleanup (no automatic TTL)
- Good fit when the event store and idempotency store share a database

See docs/idempotency-patterns.md for the comparison table.
"""

import asyncio
import logging
from datetime import datetime, timezone

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/webhooks"


# ─── Schema setup ─────────────────────────────────────────────────────────────

async def create_schema(conn: asyncpg.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_webhooks (
            id          BIGSERIAL PRIMARY KEY,
            webhook_id  TEXT NOT NULL,
            topic       TEXT NOT NULL,
            shop_domain TEXT NOT NULL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            outcome     TEXT NOT NULL DEFAULT 'processed',
            CONSTRAINT processed_webhooks_outcome_check
                CHECK (outcome IN ('processed', 'failed', 'skipped'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS processed_webhooks_webhook_id_idx
            ON processed_webhooks (webhook_id);
        CREATE INDEX IF NOT EXISTS processed_webhooks_received_at_idx
            ON processed_webhooks (received_at);
    """)


# ─── Idempotency check via INSERT ─────────────────────────────────────────────

async def mark_received(
    conn: asyncpg.Connection,
    webhook_id: str,
    topic: str,
    shop_domain: str,
) -> bool:
    """
    Attempt to INSERT a new idempotency record.

    Returns True if the INSERT succeeded (new event).
    Returns False if a UniqueViolationError was raised (duplicate).

    The unique index makes this atomic at the database level:
    concurrent inserts with the same webhook_id will serialize,
    and only one will succeed.
    """
    try:
        await conn.execute(
            """
            INSERT INTO processed_webhooks (webhook_id, topic, shop_domain)
            VALUES ($1, $2, $3)
            """,
            webhook_id, topic, shop_domain
        )
        return True  # new event
    except asyncpg.UniqueViolationError:
        return False  # duplicate


async def mark_failed(
    conn: asyncpg.Connection,
    webhook_id: str,
) -> None:
    """
    Update the outcome to 'failed' for an event that failed processing.

    Unlike the Redis pattern, we do not delete the record on failure —
    we update it. The record stays in the table for audit purposes.

    If we want future retry attempts to be accepted as new events,
    we must delete the record (see delete_for_retry below).

    If we want to track the failure but not allow retry via idempotency
    (because retry is handled by the job queue), keep the record and
    let the worker manage retries independently of the idempotency table.
    """
    await conn.execute(
        "UPDATE processed_webhooks SET outcome = 'failed' WHERE webhook_id = $1",
        webhook_id
    )


async def delete_for_retry(
    conn: asyncpg.Connection,
    webhook_id: str,
    reason: str,
) -> bool:
    """
    Delete the idempotency record to allow re-processing.

    Use when you need a webhook to be treated as new on the next delivery.
    This is the PostgreSQL equivalent of Redis clear_for_replay.

    Document every use. Include the reason why reprocessing is safe.
    """
    result = await conn.execute(
        "DELETE FROM processed_webhooks WHERE webhook_id = $1",
        webhook_id
    )
    deleted = result != "DELETE 0"
    log.warning(
        "idempotency.postgres.record_deleted_for_retry",
        webhook_id=webhook_id,
        reason=reason,
        was_present=deleted,
    )
    return deleted


# ─── Inspection ───────────────────────────────────────────────────────────────

async def is_known(conn: asyncpg.Connection, webhook_id: str) -> bool:
    """Check whether a webhook_id has been received (any outcome)."""
    row = await conn.fetchrow(
        "SELECT 1 FROM processed_webhooks WHERE webhook_id = $1",
        webhook_id
    )
    return row is not None


async def get_status(
    conn: asyncpg.Connection,
    webhook_id: str,
) -> dict | None:
    """Return the full record for a webhook_id, or None if not found."""
    row = await conn.fetchrow(
        "SELECT webhook_id, topic, shop_domain, received_at, outcome FROM processed_webhooks WHERE webhook_id = $1",
        webhook_id
    )
    return dict(row) if row else None


# ─── Cleanup ──────────────────────────────────────────────────────────────────

async def cleanup_expired(
    conn: asyncpg.Connection,
    retention_hours: int = 72,
) -> int:
    """
    Delete idempotency records older than retention_hours.

    Run periodically to prevent unbounded table growth.
    The retention window must exceed Shopify's retry window.
    """
    result = await conn.execute(
        f"DELETE FROM processed_webhooks WHERE received_at < NOW() - INTERVAL '{retention_hours} hours'"
    )
    deleted_count = int(result.split()[-1])
    log.info(
        "idempotency.postgres.cleanup_complete",
        deleted=deleted_count,
        retention_hours=retention_hours,
    )
    return deleted_count


# ─── Demo ──────────────────────────────────────────────────────────────────────

async def _demo():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    async with pool.acquire() as conn:
        await create_schema(conn)

        webhook_id = f"demo-pg-{int(datetime.now(timezone.utc).timestamp())}"
        print(f"Webhook ID: {webhook_id}")

        # First insertion — should succeed
        is_new = await mark_received(conn, webhook_id, "orders/create", "demo.myshopify.com")
        print(f"First INSERT — is_new: {is_new}")  # True

        # Second insertion — should detect duplicate
        is_new = await mark_received(conn, webhook_id, "orders/create", "demo.myshopify.com")
        print(f"Second INSERT — is_new: {is_new}")  # False

        # Inspect status
        status = await get_status(conn, webhook_id)
        print(f"Status: {status}")

        # Simulate failure and mark
        await mark_failed(conn, webhook_id)
        status = await get_status(conn, webhook_id)
        print(f"After mark_failed — outcome: {status['outcome']}")

        # Delete for retry
        deleted = await delete_for_retry(conn, webhook_id, reason="demo cleanup")
        print(f"Deleted for retry: {deleted}")

        # Now INSERT should succeed again
        is_new = await mark_received(conn, webhook_id, "orders/create", "demo.myshopify.com")
        print(f"After delete — is_new: {is_new}")  # True

        # Final cleanup
        await conn.execute("DELETE FROM processed_webhooks WHERE webhook_id = $1", webhook_id)

    await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_demo())

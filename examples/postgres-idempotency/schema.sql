-- PostgreSQL idempotency schema.
--
-- The unique index on webhook_id is the idempotency mechanism.
-- A second INSERT with the same webhook_id raises a UniqueViolation,
-- which the application catches and treats as a duplicate event.
--
-- This pattern provides durability guarantees stronger than Redis:
-- the idempotency record survives Redis restarts or failures.
-- Trade-off: higher latency (disk write vs. in-memory set).

CREATE TABLE IF NOT EXISTS processed_webhooks (
    id          BIGSERIAL PRIMARY KEY,
    webhook_id  TEXT NOT NULL,
    topic       TEXT NOT NULL,
    shop_domain TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- outcome: 'processed' | 'failed' | 'skipped'
    outcome     TEXT NOT NULL DEFAULT 'processed',
    CONSTRAINT processed_webhooks_outcome_check
        CHECK (outcome IN ('processed', 'failed', 'skipped'))
);

-- The unique constraint is the idempotency guarantee.
CREATE UNIQUE INDEX IF NOT EXISTS processed_webhooks_webhook_id_idx
    ON processed_webhooks (webhook_id);

-- Used by cleanup job to delete expired records.
-- Index covers the query: WHERE received_at < NOW() - INTERVAL.
CREATE INDEX IF NOT EXISTS processed_webhooks_received_at_idx
    ON processed_webhooks (received_at);


-- Cleanup: remove records older than the retention window.
--
-- Unlike Redis, PostgreSQL records do not expire automatically.
-- Run this periodically (e.g., daily via cron or pg_cron) to prevent
-- unbounded table growth.
--
-- The retention window must exceed Shopify's retry window to ensure
-- duplicates are caught across all retry attempts. Verify the current
-- retry window in Shopify's documentation.
--
-- Example: delete records older than 72 hours (3 days with margin)
--   DELETE FROM processed_webhooks
--   WHERE received_at < NOW() - INTERVAL '72 hours';

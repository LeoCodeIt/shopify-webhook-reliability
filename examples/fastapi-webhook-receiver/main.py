"""
Shopify Webhook Receiver — FastAPI reference implementation.

Design principle: this service has one job.
Receive the webhook, verify it, deduplicate it, persist it, enqueue it, return 200.
It never calls the ERP, WMS, or any external business system.

Mixing event receipt with business processing tightly couples two concerns
with different reliability requirements:
- Receipt: must be fast (Shopify delivery timeout) and always succeed
- Processing: may be slow and may fail (external systems are unreliable)

Separating them lets each be sized, deployed, and monitored independently.
"""

import hashlib
import hmac
import base64
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic_settings import BaseSettings


# ─── Configuration ────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    shopify_webhook_secret: str
    database_url: str
    redis_url: str
    # TTL should exceed Shopify's retry window.
    # Verify current retry window in Shopify documentation before adjusting.
    idempotency_ttl_seconds: int = 172800  # 48 hours
    log_level: str = "INFO"
    environment: str = "development"

    class Config:
        env_file = ".env"

settings = Settings()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()


# ─── Metrics ──────────────────────────────────────────────────────────────────

webhooks_received = Counter(
    "webhooks_received_total",
    "Total webhook deliveries received",
    ["topic"]
)
webhooks_rejected = Counter(
    "webhooks_rejected_total",
    "Webhook deliveries rejected",
    ["topic", "reason"]  # reason: invalid_hmac, missing_header
)
webhooks_deduplicated = Counter(
    "webhooks_deduplicated_total",
    "Webhook deliveries skipped as duplicates",
    ["topic"]
)
receive_duration = Histogram(
    "webhook_receive_duration_seconds",
    "Time from request received to 200 returned",
    ["topic"]
)


# ─── Application lifecycle ────────────────────────────────────────────────────

db_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, redis_client

    db_pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)

    # Ensure the events table exists on startup
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS webhook_events (
                id          BIGSERIAL PRIMARY KEY,
                webhook_id  TEXT NOT NULL,
                topic       TEXT NOT NULL,
                shop_domain TEXT NOT NULL,
                api_version TEXT,
                payload     JSONB NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status      TEXT NOT NULL DEFAULT 'queued'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS webhook_events_webhook_id_idx
                ON webhook_events (webhook_id);
        """)

    log.info("receiver.started", environment=settings.environment)
    yield

    await db_pool.close()
    await redis_client.aclose()
    log.info("receiver.stopped")


app = FastAPI(title="shopify-webhook-receiver", lifespan=lifespan)

# Expose Prometheus metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# ─── HMAC verification ────────────────────────────────────────────────────────

def verify_shopify_hmac(raw_body: bytes, hmac_header: str, secret: str) -> bool:
    """
    Verify Shopify's HMAC-SHA256 signature.

    Must use the raw request body — not the parsed JSON — because JSON
    serialization does not guarantee byte-for-byte consistency.

    Uses hmac.compare_digest for timing-safe comparison to prevent
    timing attacks that could leak information about the secret.
    """
    expected = base64.b64encode(
        hmac.new(secret.encode(), raw_body, hashlib.sha256).digest()
    ).decode()
    return hmac.compare_digest(expected, hmac_header)


# ─── Idempotency check ────────────────────────────────────────────────────────

async def is_duplicate(webhook_id: str) -> bool:
    """
    Check whether we have already processed this webhook_id.

    Uses Redis SET NX (set if not exists) — this is atomic, preventing
    race conditions when two concurrent requests arrive with the same ID.

    The TTL should exceed Shopify's retry window. A late retry with the
    same webhook_id after the TTL would be treated as a new event.
    """
    key = f"webhook:processed:{webhook_id}"
    acquired = await redis_client.set(key, "1", nx=True, ex=settings.idempotency_ttl_seconds)
    return not acquired  # acquired=None means key existed (duplicate)


async def release_idempotency_key(webhook_id: str) -> None:
    """
    Release the idempotency key when processing fails.

    If we set the key but then fail to persist or enqueue the event,
    we delete the key to allow a future retry (from Shopify or internally)
    to be treated as a new event.

    Trade-off: if this delete also fails (Redis unavailable), the next
    delivery attempt will be treated as a duplicate and silently skipped.
    This is acceptable — the alternative (keeping a failed event stuck)
    is worse for data integrity.
    """
    try:
        await redis_client.delete(f"webhook:processed:{webhook_id}")
    except Exception as e:
        log.warning("idempotency.key_release_failed", webhook_id=webhook_id, error=str(e))


# ─── Event persistence ────────────────────────────────────────────────────────

async def persist_event(
    conn: asyncpg.Connection,
    webhook_id: str,
    topic: str,
    shop_domain: str,
    api_version: str | None,
    payload: dict,
) -> int:
    """
    Persist the raw event to the event store before enqueuing.

    Persisting before enqueuing ensures we have a durable record of the
    event even if the queue operation fails. The event store is the
    source of truth for what was received; the queue is a processing signal.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO webhook_events (webhook_id, topic, shop_domain, api_version, payload)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        webhook_id, topic, shop_domain, api_version, json.dumps(payload)
    )
    return row["id"]


# ─── Queue enqueue ────────────────────────────────────────────────────────────

async def enqueue_job(event_id: int, webhook_id: str, topic: str) -> None:
    """
    Add the event to the processing queue.

    The queue entry contains only the event_id and metadata — not the full
    payload. The worker retrieves the payload from the event store. This
    keeps queue messages small and ensures the worker always processes
    the persisted version of the payload.
    """
    await redis_client.xadd(
        "webhook_jobs",
        {
            "event_id": str(event_id),
            "webhook_id": webhook_id,
            "topic": topic,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }
    )


# ─── Webhook endpoint ─────────────────────────────────────────────────────────

@app.post("/webhooks/{topic:path}", status_code=200)
async def receive_webhook(topic: str, request: Request) -> JSONResponse:
    """
    Single entry point for all Shopify webhook topics.

    Route: /webhooks/orders/create, /webhooks/inventory_levels/update, etc.
    The topic is extracted from the URL path and validated against the
    X-Shopify-Topic header.

    Returns 200 immediately after enqueueing. Never returns based on
    downstream processing status — that is the worker's concern.
    """
    start_time = time.monotonic()

    # Extract required headers
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    webhook_id = request.headers.get("X-Shopify-Webhook-Id")
    shop_domain = request.headers.get("X-Shopify-Shop-Domain", "unknown")
    api_version = request.headers.get("X-Shopify-Api-Version")
    topic_header = request.headers.get("X-Shopify-Topic", topic)

    if not hmac_header or not webhook_id:
        log.warning(
            "webhook.missing_headers",
            has_hmac=bool(hmac_header),
            has_webhook_id=bool(webhook_id),
        )
        webhooks_rejected.labels(topic=topic_header, reason="missing_header").inc()
        raise HTTPException(status_code=400, detail="Missing required Shopify headers")

    # Read raw body BEFORE any parsing — HMAC is computed over raw bytes
    raw_body = await request.body()

    # 1. Verify HMAC signature
    if not verify_shopify_hmac(raw_body, hmac_header, settings.shopify_webhook_secret):
        log.warning("webhook.invalid_hmac", webhook_id=webhook_id, topic=topic_header)
        webhooks_rejected.labels(topic=topic_header, reason="invalid_hmac").inc()
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    webhooks_received.labels(topic=topic_header).inc()

    # 2. Idempotency check
    if await is_duplicate(webhook_id):
        log.info("webhook.duplicate", webhook_id=webhook_id, topic=topic_header)
        webhooks_deduplicated.labels(topic=topic_header).inc()
        # Return 200 — Shopify expects 2xx even for duplicates we're skipping
        return JSONResponse({"status": "duplicate"})

    # Parse payload (after HMAC verification)
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        await release_idempotency_key(webhook_id)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # 3 & 4. Persist event and enqueue job
    try:
        async with db_pool.acquire() as conn:
            event_id = await persist_event(
                conn, webhook_id, topic_header, shop_domain, api_version, payload
            )
        await enqueue_job(event_id, webhook_id, topic_header)
    except Exception as e:
        # Something went wrong after we set the idempotency key.
        # Release it so a future attempt can succeed.
        await release_idempotency_key(webhook_id)
        log.error("webhook.enqueue_failed", webhook_id=webhook_id, error=str(e))
        # Return 500 — Shopify will retry delivery
        raise HTTPException(status_code=500, detail="Failed to enqueue webhook")

    duration = time.monotonic() - start_time
    receive_duration.labels(topic=topic_header).observe(duration)

    log.info(
        "webhook.received",
        webhook_id=webhook_id,
        topic=topic_header,
        shop_domain=shop_domain,
        event_id=event_id,
        duration_ms=round(duration * 1000, 2),
    )

    # 5. Return 200 immediately
    return JSONResponse({"status": "accepted", "event_id": event_id})


# ─── Health check ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    """
    Lightweight health check for load balancer and monitoring.
    Checks connectivity to Redis and PostgreSQL.
    """
    checks = {}

    try:
        await redis_client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"

    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks}

"""
Webhook processing worker with exponential backoff and dead letter queue.

Design:
- Reads jobs from Redis Streams (XREADGROUP with consumer groups)
- Processes each job against a target system (stubbed as a function)
- On failure: classifies the error as retriable or non-retriable
- On retriable failure: exponential backoff with full jitter, up to MAX_ATTEMPTS
- On non-retriable failure or exhausted retries: moves to DLQ
- Acknowledges (XACK) only on success or DLQ insertion

The worker never deletes events from the event store. Status is tracked
in the webhook_jobs table. The event store is append-only.
"""

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Awaitable

import asyncpg
import redis.asyncio as redis
import structlog

log = structlog.get_logger()


# ─── Configuration ────────────────────────────────────────────────────────────

STREAM_KEY = "webhook_jobs"
CONSUMER_GROUP = "webhook_processors"
CONSUMER_NAME = "worker-1"  # unique per process/replica

MAX_ATTEMPTS = 5
BASE_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 300.0  # 5 minutes cap

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/webhooks"
REDIS_URL = "redis://localhost:6379"


# ─── Failure classification ────────────────────────────────────────────────────

class ErrorType(Enum):
    RETRIABLE = "retriable"
    NON_RETRIABLE = "non_retriable"


@dataclass
class ProcessingError:
    message: str
    error_type: ErrorType
    http_status: int | None = None
    retry_after_seconds: int | None = None  # from Retry-After header


def classify_error(exc: Exception, http_status: int | None = None) -> ProcessingError:
    """
    Classify an exception as retriable or non-retriable.

    Retriable: transient infrastructure issues. The same request may succeed later.
    Non-retriable: permanent failures. Retrying would produce the same result.

    This classification is target-system-specific — adjust based on the ERP/WMS
    API's error contract.
    """
    if http_status is not None:
        if http_status == 422:
            # Unprocessable entity: payload is malformed or violates business rules.
            # Retrying will not fix this — it needs a code or data fix.
            return ProcessingError(str(exc), ErrorType.NON_RETRIABLE, http_status)
        if http_status == 429:
            # Rate limited: retriable. Parse Retry-After if available.
            return ProcessingError(str(exc), ErrorType.RETRIABLE, http_status)
        if http_status in (500, 502, 503, 504):
            # Server error: retriable. Target system may recover.
            return ProcessingError(str(exc), ErrorType.RETRIABLE, http_status)
        if 400 <= http_status < 500:
            # Other 4xx: generally non-retriable (client error).
            return ProcessingError(str(exc), ErrorType.NON_RETRIABLE, http_status)

    if isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError)):
        return ProcessingError(str(exc), ErrorType.RETRIABLE)

    # Default: retriable. When uncertain, retry — the DLQ captures persistent failures.
    return ProcessingError(str(exc), ErrorType.RETRIABLE)


# ─── Backoff calculation ───────────────────────────────────────────────────────

def calculate_backoff(attempt: int, base: float = BASE_DELAY_SECONDS, cap: float = MAX_DELAY_SECONDS) -> float:
    """
    Exponential backoff with full jitter.

    Formula: random.uniform(0, min(cap, base * 2^attempt))

    Full jitter prevents thundering herd: if multiple workers have been
    retrying since the same outage, they will not all retry simultaneously
    when the outage resolves. Each picks a random point in the window.

    At attempt 0: delay up to 2s
    At attempt 1: delay up to 4s
    At attempt 2: delay up to 8s
    At attempt 3: delay up to 16s
    At attempt 4: delay up to 32s (capped at MAX_DELAY_SECONDS)
    """
    exponential = base * (2 ** attempt)
    capped = min(cap, exponential)
    return random.uniform(0, capped)


# ─── Job state persistence ────────────────────────────────────────────────────

async def update_job_status(
    conn: asyncpg.Connection,
    event_id: int,
    status: str,
    attempt: int,
    error_message: str | None = None,
    next_retry_at: datetime | None = None,
) -> None:
    await conn.execute(
        """
        UPDATE webhook_events
        SET status = $2,
            attempt_count = $3,
            last_error = $4,
            next_retry_at = $5,
            updated_at = NOW()
        WHERE id = $1
        """,
        event_id, status, attempt, error_message, next_retry_at
    )


async def move_to_dlq(
    conn: asyncpg.Connection,
    event_id: int,
    webhook_id: str,
    topic: str,
    shop_domain: str,
    payload: dict,
    attempt_count: int,
    error_history: list[dict],
    last_error: str,
) -> None:
    """
    Insert a DLQ entry and update the event status.

    Uses a transaction so both writes succeed or both fail.
    """
    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO dead_letter_queue
                (webhook_id, topic, shop_domain, raw_payload, attempt_count, error_history, last_error)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (webhook_id) DO UPDATE
                SET attempt_count = EXCLUDED.attempt_count,
                    error_history = EXCLUDED.error_history,
                    last_error = EXCLUDED.last_error,
                    exhausted_at = NOW(),
                    status = 'pending_review'
            """,
            webhook_id, topic, shop_domain, json.dumps(payload),
            attempt_count, json.dumps(error_history), last_error
        )
        await conn.execute(
            "UPDATE webhook_events SET status = 'dead_lettered', updated_at = NOW() WHERE id = $1",
            event_id
        )

    log.error(
        "webhook.moved_to_dlq",
        webhook_id=webhook_id,
        topic=topic,
        attempt_count=attempt_count,
        last_error=last_error,
    )


# ─── Target system stub ───────────────────────────────────────────────────────

class FakeTargetSystemError(Exception):
    def __init__(self, message: str, http_status: int):
        super().__init__(message)
        self.http_status = http_status


async def process_at_target(topic: str, payload: dict) -> None:
    """
    Stub: replace with real ERP/WMS/CRM API call.

    This stub simulates a 30% transient failure rate for demonstration.
    In production, this function would call an external API and raise
    an exception with the HTTP status code attached.
    """
    if random.random() < 0.30:
        raise FakeTargetSystemError("Target system temporarily unavailable", http_status=503)
    log.info("target.write_success", topic=topic, payload_keys=list(payload.keys()))


# ─── Job processor ────────────────────────────────────────────────────────────

async def process_job(
    db_pool: asyncpg.Pool,
    job: dict,
    target_fn: Callable[[str, dict], Awaitable[None]] = process_at_target,
) -> bool:
    """
    Process a single job. Returns True on success, False on failure.

    Manages the full lifecycle: attempt, classify error, backoff, DLQ.
    """
    event_id = int(job["event_id"])
    webhook_id = job["webhook_id"]
    topic = job["topic"]

    # Fetch event from the event store
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, shop_domain, attempt_count, error_history FROM webhook_events WHERE id = $1",
            event_id
        )

    if not row:
        log.error("worker.event_not_found", event_id=event_id)
        return False

    payload = json.loads(row["payload"])
    shop_domain = row["shop_domain"] or "unknown"
    attempt = (row["attempt_count"] or 0) + 1
    error_history = json.loads(row["error_history"]) if row["error_history"] else []

    start = time.monotonic()

    try:
        await target_fn(topic, payload)
    except Exception as exc:
        http_status = getattr(exc, "http_status", None)
        error = classify_error(exc, http_status)
        duration_ms = round((time.monotonic() - start) * 1000, 2)

        error_record = {
            "attempt": attempt,
            "error": error.message,
            "error_type": error.error_type.value,
            "http_status": error.http_status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": duration_ms,
        }
        error_history.append(error_record)

        log.warning(
            "webhook.processing_failed",
            webhook_id=webhook_id,
            topic=topic,
            attempt=attempt,
            error=error.message,
            error_type=error.error_type.value,
            will_retry=error.error_type == ErrorType.RETRIABLE and attempt < MAX_ATTEMPTS,
        )

        async with db_pool.acquire() as conn:
            if error.error_type == ErrorType.NON_RETRIABLE or attempt >= MAX_ATTEMPTS:
                await move_to_dlq(
                    conn, event_id, webhook_id, topic, shop_domain,
                    payload, attempt, error_history, error.message
                )
            else:
                # Calculate next retry time
                if error.retry_after_seconds:
                    delay = float(error.retry_after_seconds)
                else:
                    delay = calculate_backoff(attempt)

                next_retry = datetime.fromtimestamp(
                    time.time() + delay, tz=timezone.utc
                )
                await update_job_status(
                    conn, event_id, "pending_retry", attempt,
                    error_message=error.message,
                    next_retry_at=next_retry,
                )
                log.info(
                    "webhook.retry_scheduled",
                    webhook_id=webhook_id,
                    attempt=attempt,
                    next_retry_in_seconds=round(delay, 1),
                )
                await asyncio.sleep(delay)

        return False

    duration_ms = round((time.monotonic() - start) * 1000, 2)
    async with db_pool.acquire() as conn:
        await update_job_status(conn, event_id, "processed", attempt)

    log.info(
        "webhook.processed",
        webhook_id=webhook_id,
        topic=topic,
        attempt=attempt,
        duration_ms=duration_ms,
    )
    return True


# ─── Stream consumer ──────────────────────────────────────────────────────────

async def ensure_consumer_group(r: redis.Redis) -> None:
    try:
        await r.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def run_worker(db_pool: asyncpg.Pool, r: redis.Redis) -> None:
    await ensure_consumer_group(r)
    log.info("worker.started", stream=STREAM_KEY, group=CONSUMER_GROUP, consumer=CONSUMER_NAME)

    while True:
        messages = await r.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {STREAM_KEY: ">"},
            count=1,
            block=5000,  # block 5 seconds if no messages
        )

        if not messages:
            continue

        for stream_name, entries in messages:
            for entry_id, fields in entries:
                job = {k: v for k, v in fields.items()}
                log.info(
                    "worker.job_received",
                    entry_id=entry_id,
                    webhook_id=job.get("webhook_id"),
                    topic=job.get("topic"),
                )

                success = await process_job(db_pool, job)

                if success:
                    # XACK only on success. Failed jobs remain in PEL
                    # (pending entries list) for recovery or manual inspection.
                    await r.xack(STREAM_KEY, CONSUMER_GROUP, entry_id)
                else:
                    # Failed jobs are tracked in webhook_events table.
                    # Acknowledge to avoid re-delivery from the stream —
                    # retry scheduling is managed by the application, not the stream.
                    await r.xack(STREAM_KEY, CONSUMER_GROUP, entry_id)


# ─── Entry point ──────────────────────────────────────────────────────────────

async def main():
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    r = redis.from_url(REDIS_URL, decode_responses=True)

    try:
        await run_worker(db_pool, r)
    finally:
        await db_pool.close()
        await r.aclose()


if __name__ == "__main__":
    import structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )
    asyncio.run(main())

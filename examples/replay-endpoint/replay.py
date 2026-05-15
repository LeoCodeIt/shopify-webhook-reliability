"""
DLQ replay endpoint — FastAPI implementation.

Provides authenticated HTTP endpoints for inspecting the dead letter queue
and re-enqueuing events for processing.

IMPORTANT: This endpoint must not be publicly accessible.
Deploy behind a VPN or private network boundary, and combine
API key authentication with network-level access control.
"""

import json
import logging
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as redis
import structlog
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

log = structlog.get_logger()


# ─── Configuration ────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    database_url: str
    redis_url: str
    replay_api_key: str

    class Config:
        env_file = ".env"

settings = Settings()


# ─── Authentication ───────────────────────────────────────────────────────────

api_key_header = APIKeyHeader(name="X-Replay-Api-Key", auto_error=True)

async def require_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Validate the replay API key.

    API key alone is insufficient for production — combine with
    network-level access control (VPN, private subnet, IP allowlist).
    """
    if api_key != settings.replay_api_key:
        log.warning("replay.unauthorized_attempt")
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key


# ─── Application lifecycle ────────────────────────────────────────────────────

from contextlib import asynccontextmanager

db_pool: asyncpg.Pool | None = None
redis_client: redis.Redis | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=5)
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    log.info("replay_endpoint.started")
    yield
    await db_pool.close()
    await redis_client.aclose()

app = FastAPI(title="shopify-webhook-replay", lifespan=lifespan)


# ─── Request / Response models ────────────────────────────────────────────────

class ReplayRequest(BaseModel):
    dlq_id: str = Field(..., description="UUID of the DLQ entry to replay")


class BulkReplayRequest(BaseModel):
    topic: str = Field(..., description="Webhook topic to filter by (e.g. orders/create)")
    since: datetime = Field(..., description="Start of the exhausted_at window (UTC)")
    until: datetime = Field(..., description="End of the exhausted_at window (UTC)")
    dry_run: bool = Field(True, description="If true, list entries without re-enqueuing")


class ForceReplayRequest(BaseModel):
    dlq_id: str = Field(..., description="UUID of the DLQ entry to replay")
    webhook_id: str = Field(..., description="webhook_id to clear from idempotency store")
    reason: str = Field(..., description="Why idempotency override is safe — this is logged")


class DiscardRequest(BaseModel):
    dlq_id: str = Field(..., description="UUID of the DLQ entry to discard")
    reason: str = Field(..., description="Why this entry is being discarded — audit record")


# ─── DLQ operations ───────────────────────────────────────────────────────────

async def _replay_entry(conn: asyncpg.Connection, dlq_id: str) -> bool:
    """
    Mark entry as 'replaying' and re-enqueue to the processing stream.

    Uses a conditional UPDATE (status = 'pending_review') to prevent
    concurrent replay of the same entry.
    """
    row = await conn.fetchrow(
        """
        UPDATE dead_letter_queue
        SET status = 'replaying', resolved_at = NOW()
        WHERE id = $1::uuid AND status = 'pending_review'
        RETURNING id, raw_payload, topic, webhook_id
        """,
        dlq_id
    )
    if not row:
        return False  # entry not found or already being replayed

    await redis_client.xadd(
        "webhook_jobs",
        {
            "webhook_id": row["webhook_id"],
            "topic": row["topic"],
            "payload": row["raw_payload"],
            "source": "dlq_replay",
            "dlq_id": str(row["id"]),
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    log.info(
        "replay.enqueued",
        dlq_id=dlq_id,
        webhook_id=row["webhook_id"],
        topic=row["topic"],
    )
    return True


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/dlq", dependencies=[Depends(require_api_key)])
async def list_dlq(
    status: str = "pending_review",
    topic: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    List DLQ entries. Defaults to pending_review.

    Query parameters:
    - status: pending_review | replaying | resolved | discarded
    - topic: filter by webhook topic (optional)
    - limit: max entries to return (default 50, max 200)
    """
    limit = min(limit, 200)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, webhook_id, topic, shop_domain, last_error,
                   attempt_count, exhausted_at, status, resolution_note, resolved_at
            FROM dead_letter_queue
            WHERE status = $1
              AND ($2::text IS NULL OR topic = $2)
            ORDER BY exhausted_at DESC
            LIMIT $3
            """,
            status, topic, limit
        )
    return [dict(r) for r in rows]


@app.get("/dlq/{dlq_id}", dependencies=[Depends(require_api_key)])
async def get_dlq_entry(dlq_id: str) -> dict:
    """Return a single DLQ entry including full error_history and raw_payload."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM dead_letter_queue WHERE id = $1::uuid",
            dlq_id
        )
    if not row:
        raise HTTPException(status_code=404, detail="DLQ entry not found")
    return dict(row)


@app.post("/dlq/replay", dependencies=[Depends(require_api_key)])
async def replay_single(body: ReplayRequest) -> dict:
    """
    Re-enqueue a single DLQ entry.

    The worker processes it using current code — including any fixes
    deployed after the original failure.
    """
    async with db_pool.acquire() as conn:
        success = await _replay_entry(conn, body.dlq_id)

    if not success:
        raise HTTPException(
            status_code=409,
            detail="Entry not found or not in pending_review status"
        )
    return {"status": "enqueued", "dlq_id": body.dlq_id}


@app.post("/dlq/replay/bulk", dependencies=[Depends(require_api_key)])
async def replay_bulk(body: BulkReplayRequest) -> dict:
    """
    Replay all pending_review entries matching topic and time window.

    dry_run=True (default): returns the list of entries that would be replayed.
    dry_run=False: re-enqueues entries and marks them as 'replaying'.

    Always dry_run first. Verify the scope before committing.
    """
    async with db_pool.acquire() as conn:
        entries = await conn.fetch(
            """
            SELECT id, webhook_id, topic, exhausted_at, last_error
            FROM dead_letter_queue
            WHERE topic = $1
              AND exhausted_at BETWEEN $2 AND $3
              AND status = 'pending_review'
            ORDER BY exhausted_at ASC
            """,
            body.topic, body.since, body.until
        )

        if body.dry_run:
            return {
                "dry_run": True,
                "would_replay": len(entries),
                "entries": [
                    {
                        "id": str(e["id"]),
                        "webhook_id": e["webhook_id"],
                        "exhausted_at": e["exhausted_at"].isoformat(),
                        "last_error": e["last_error"],
                    }
                    for e in entries
                ]
            }

        replayed = 0
        failed = 0
        for entry in entries:
            success = await _replay_entry(conn, str(entry["id"]))
            if success:
                replayed += 1
            else:
                failed += 1

    log.info(
        "replay.bulk_complete",
        topic=body.topic,
        replayed=replayed,
        failed=failed,
    )
    return {"dry_run": False, "replayed": replayed, "failed_to_enqueue": failed}


@app.post("/dlq/replay/force", dependencies=[Depends(require_api_key)])
async def force_replay(body: ForceReplayRequest) -> dict:
    """
    Clear the idempotency key and re-enqueue.

    Use only when you are certain the event needs to be reprocessed —
    for example, when a processing bug caused silent failure after the
    idempotency key was set.

    The reason field is required and is logged permanently.
    Document every forced replay.
    """
    # Delete idempotency key from Redis
    key = f"webhook:processed:{body.webhook_id}"
    deleted = await redis_client.delete(key)

    log.warning(
        "replay.force_idempotency_cleared",
        webhook_id=body.webhook_id,
        dlq_id=body.dlq_id,
        reason=body.reason,
        key_was_present=bool(deleted),
    )

    async with db_pool.acquire() as conn:
        success = await _replay_entry(conn, body.dlq_id)

    if not success:
        raise HTTPException(
            status_code=409,
            detail="Entry not found or not in pending_review status"
        )

    return {
        "status": "force_replayed",
        "dlq_id": body.dlq_id,
        "webhook_id": body.webhook_id,
        "idempotency_key_cleared": bool(deleted),
    }


@app.post("/dlq/discard", dependencies=[Depends(require_api_key)])
async def discard_entry(body: DiscardRequest) -> dict:
    """
    Mark an entry as discarded. Requires a reason — this is an audit record.

    Use for genuinely unrecoverable events: test events, known Shopify bugs,
    or events that predate a schema change that cannot be backfilled.
    """
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE dead_letter_queue
            SET status = 'discarded',
                resolution_note = $2,
                resolved_at = NOW()
            WHERE id = $1::uuid AND status = 'pending_review'
            """,
            body.dlq_id, body.reason
        )

    if result == "UPDATE 0":
        raise HTTPException(
            status_code=409,
            detail="Entry not found or not in pending_review status"
        )

    log.info(
        "replay.entry_discarded",
        dlq_id=body.dlq_id,
        reason=body.reason,
    )
    return {"status": "discarded", "dlq_id": body.dlq_id}


@app.get("/health")
async def health() -> dict:
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

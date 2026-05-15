"""
Redis-based idempotency patterns for webhook processing.

Three patterns demonstrated:
1. SET NX — the primary pattern used in the receiver
2. SET NX with delete-on-failure — safe retry on processing error
3. Batch check — inspect multiple IDs without marking them

The SET NX pattern is atomic: check and set happen in a single
Redis command. There is no TOCTOU race condition.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Awaitable

import redis.asyncio as redis

log = logging.getLogger(__name__)

IDEMPOTENCY_KEY_PREFIX = "webhook:processed"
DEFAULT_TTL_SECONDS = 172800  # 48 hours — should exceed Shopify's retry window


def _key(webhook_id: str) -> str:
    return f"{IDEMPOTENCY_KEY_PREFIX}:{webhook_id}"


# ─── Pattern 1: SET NX ────────────────────────────────────────────────────────

async def check_and_mark(
    r: redis.Redis,
    webhook_id: str,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> bool:
    """
    Atomic check-and-mark. Returns True if this is a duplicate.

    SET NX: set if not exists. The return value distinguishes:
    - True (key was set): this is a new event — process it
    - None (key already existed): this is a duplicate — skip it

    Single-command atomicity ensures that two concurrent requests
    with the same webhook_id cannot both see "new event".
    """
    acquired = await r.set(_key(webhook_id), "1", nx=True, ex=ttl_seconds)
    is_duplicate = acquired is None
    return is_duplicate


# ─── Pattern 2: SET NX with delete-on-failure ─────────────────────────────────

async def process_with_idempotency(
    r: redis.Redis,
    webhook_id: str,
    process_fn: Callable[[], Awaitable[None]],
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> str:
    """
    Run process_fn exactly once per webhook_id.

    Returns:
    - "processed": process_fn ran successfully
    - "duplicate": webhook_id was already marked, skipped
    - "failed": process_fn raised an exception; idempotency key released

    If process_fn fails and the key is released, a future call with
    the same webhook_id will re-run process_fn. This is the desired
    behavior: transient failures should be retried, not silently dropped.

    Trade-off: if both process_fn and the key release fail (Redis down),
    the next delivery is treated as a duplicate. The alternative —
    leaving a failed event permanently marked as processed — causes
    silent data loss, which is worse.
    """
    acquired = await r.set(_key(webhook_id), "1", nx=True, ex=ttl_seconds)
    if acquired is None:
        return "duplicate"

    try:
        await process_fn()
        return "processed"
    except Exception as e:
        log.warning(
            "idempotency.releasing_on_failure",
            webhook_id=webhook_id,
            error=str(e),
        )
        await _release_key(r, webhook_id)
        return "failed"


async def _release_key(r: redis.Redis, webhook_id: str) -> None:
    try:
        await r.delete(_key(webhook_id))
    except Exception as e:
        log.error(
            "idempotency.release_failed",
            webhook_id=webhook_id,
            error=str(e),
        )


# ─── Pattern 3: Batch inspection ──────────────────────────────────────────────

@dataclass
class IdempotencyStatus:
    webhook_id: str
    is_marked: bool
    ttl_seconds: int | None  # None if key does not exist


async def inspect_keys(
    r: redis.Redis,
    webhook_ids: list[str],
) -> list[IdempotencyStatus]:
    """
    Check the status of multiple idempotency keys without marking them.

    Used for debugging and DLQ replay decisions. Does not modify state.

    For each webhook_id:
    - is_marked=True: the key exists (event was received and accepted)
    - is_marked=False: the key does not exist (event not yet seen, or TTL expired)
    - ttl_seconds: remaining TTL (-2 if key not found, -1 if no TTL)
    """
    pipeline = r.pipeline()
    for wid in webhook_ids:
        pipeline.ttl(_key(wid))
    ttls = await pipeline.execute()

    results = []
    for wid, ttl in zip(webhook_ids, ttls):
        is_marked = ttl != -2  # -2 means key does not exist
        results.append(IdempotencyStatus(
            webhook_id=wid,
            is_marked=is_marked,
            ttl_seconds=ttl if is_marked else None,
        ))
    return results


# ─── Force-clear for replay ────────────────────────────────────────────────────

async def clear_for_replay(r: redis.Redis, webhook_id: str, reason: str) -> bool:
    """
    Explicitly delete an idempotency key to allow re-processing.

    Use only when you are certain the event needs to be reprocessed —
    for example, when a processing bug caused silent failure after the
    idempotency key was set.

    Document every use: this is an intentional override of a safety mechanism.
    The reason parameter is logged; it should describe why reprocessing is safe.
    """
    deleted = await r.delete(_key(webhook_id))
    log.warning(
        "idempotency.force_cleared",
        webhook_id=webhook_id,
        reason=reason,
        was_present=bool(deleted),
    )
    return bool(deleted)


# ─── Demo ──────────────────────────────────────────────────────────────────────

async def _demo():
    r = redis.from_url("redis://localhost:6379", decode_responses=True)

    webhook_id = f"demo-{int(datetime.now(timezone.utc).timestamp())}"
    print(f"Webhook ID: {webhook_id}")

    # First call — should mark as new
    is_dup = await check_and_mark(r, webhook_id, ttl_seconds=60)
    print(f"First call — is_duplicate: {is_dup}")  # False

    # Second call — should detect duplicate
    is_dup = await check_and_mark(r, webhook_id, ttl_seconds=60)
    print(f"Second call — is_duplicate: {is_dup}")  # True

    # Inspect
    statuses = await inspect_keys(r, [webhook_id, "nonexistent-id"])
    for s in statuses:
        print(f"  {s.webhook_id}: marked={s.is_marked}, ttl={s.ttl_seconds}s")

    # Force clear and retry
    await clear_for_replay(r, webhook_id, reason="demo — testing force clear")
    is_dup = await check_and_mark(r, webhook_id, ttl_seconds=60)
    print(f"After clear — is_duplicate: {is_dup}")  # False

    # Cleanup
    await r.delete(_key(webhook_id))
    await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_demo())

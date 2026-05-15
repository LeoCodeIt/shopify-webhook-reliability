# Retry Strategy

Not all failures should be retried. Not all retriable failures should be retried with the same timing. A retry strategy that does not distinguish between failure types will either retry non-recoverable errors indefinitely or give up on recoverable errors too quickly.

---

## Failure Classification

The first decision in a retry strategy is whether a failure is retriable.

**Retriable failures** — transient conditions that may resolve without intervention:
- HTTP 429 (rate limited) — the ERP is throttling requests
- HTTP 503 / 504 (service unavailable / gateway timeout) — the ERP is temporarily down or slow
- Connection timeout — network issue, usually transient
- Connection refused — ERP may be restarting

**Non-retriable failures** — permanent conditions that will not resolve by retrying:
- HTTP 401 / 403 (authentication / authorization) — credentials have not changed between retries
- HTTP 400 (bad request) — the payload is malformed; retrying sends the same bad payload
- Schema validation error — the payload does not match the ERP's expected format
- Business rule violation (e.g., ERP rejects duplicate external ID) — retry will produce the same rejection

```python
def is_retriable(error: Exception) -> bool:
    """Classify whether a processing failure should be retried."""
    if isinstance(error, httpx.TimeoutException):
        return True
    if isinstance(error, httpx.HTTPStatusError):
        # Retry on server errors and rate limits, not on client errors
        return error.response.status_code in {429, 500, 502, 503, 504}
    if isinstance(error, (ConnectionError, OSError)):
        return True
    # Schema errors, validation errors, auth errors: do not retry
    return False
```

Non-retriable failures should go directly to the dead letter queue with the error classified. Retrying them wastes resources and delays attention to a condition that requires human intervention.

---

## Exponential Backoff

Retrying immediately after a failure often produces another failure — the upstream system has not had time to recover. Exponential backoff spaces retries with increasing delays:

```
attempt 0: process
attempt 1: wait 2s,  retry
attempt 2: wait 4s,  retry
attempt 3: wait 8s,  retry
attempt 4: wait 16s, retry
attempt 5: wait 32s, retry
→ dead letter queue
```

The base formula: `delay = base_delay * (2 ** attempt_number)`

**Add jitter.** When many jobs fail simultaneously (e.g., ERP goes down, 500 queued events all fail at once), exponential backoff without jitter causes a thundering herd: all retries fire at the same time after the backoff period. Adding a random component distributes the retries over time:

```python
import random

def calculate_backoff(attempt: int, base_delay: float = 2.0, max_delay: float = 300.0) -> float:
    """
    Exponential backoff with full jitter.

    Full jitter (random between 0 and the calculated delay) distributes retry
    attempts more evenly than adding a small random offset to the base calculation.

    Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    return random.uniform(0, delay)
```

---

## Max Attempts Configuration

The maximum number of retry attempts depends on the expected recovery time of the target system.

A target system with a maximum maintenance window of 2 hours, with retries spaced by exponential backoff starting at 2 seconds, will exhaust 5 attempts in approximately 1 minute. That is insufficient to survive the maintenance window.

Calculate the maximum backoff coverage before setting `MAX_ATTEMPTS`:

```
5 attempts:  ~60 seconds total backoff
8 attempts:  ~8 minutes total backoff
12 attempts: ~2 hours total backoff
```

For ERP integrations where maintenance windows can extend several hours, a higher attempt count with a capped maximum delay is more appropriate than a lower attempt count:

```python
MAX_ATTEMPTS = 10
MAX_DELAY_SECONDS = 600  # cap at 10 minutes per attempt

def calculate_backoff(attempt: int) -> float:
    delay = min(2.0 * (2 ** attempt), MAX_DELAY_SECONDS)
    return random.uniform(delay * 0.5, delay)  # jitter in the upper half
```

---

## Rate Limit Handling

When the target system returns HTTP 429, it may include a `Retry-After` header specifying how long to wait before retrying. Respect this header when present — it represents the upstream system's own guidance on retry timing.

```python
async def handle_rate_limit(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass  # fallback to exponential backoff
    return calculate_backoff(attempt)
```

If the ERP does not include `Retry-After`, use the standard backoff. The important thing is to not retry immediately — a 429 means the upstream is already overloaded.

---

## Persisting Retry State

Retry state (attempt count, last error, next retry time) must be persisted durably. If the worker crashes after attempt 3, the next worker must know to continue from attempt 4 — not restart from attempt 0.

Store retry state in the event store or a dedicated job store, not in process memory.

```python
# When a job fails and is re-queued for retry
await db.execute(
    """
    UPDATE webhook_jobs
    SET
        attempt_count = attempt_count + 1,
        last_error = $1,
        last_error_at = NOW(),
        next_retry_at = NOW() + $2::interval,
        status = 'pending'
    WHERE id = $3
    """,
    str(error),
    f"{backoff_seconds} seconds",
    job_id
)
```

---

## Dead Letter Queue Transition

When `attempt_count >= MAX_ATTEMPTS`, the job moves to the dead letter queue rather than being re-queued:

```python
async def handle_job_failure(job_id: str, error: Exception, attempt: int):
    if not is_retriable(error):
        await move_to_dlq(job_id, error, reason="non_retriable")
        return

    if attempt >= MAX_ATTEMPTS:
        await move_to_dlq(job_id, error, reason="max_attempts_exhausted")
        return

    backoff = calculate_backoff(attempt)
    await reschedule_job(job_id, delay_seconds=backoff, attempt=attempt + 1)
```

The DLQ entry should include the full error history, not just the final error. The pattern of failures (retry 1: 503, retry 2: 503, retry 3: 503, retry 4: 400) tells a different story than just the last error.

See `docs/dead-letter-queues.md` for DLQ design.

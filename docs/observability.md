# Observability

A webhook integration without observability is a black box. The failure mode is: a merchant notices missing orders two hours after they stopped flowing, an engineer spends an hour tracing back through logs to find the ERP was rate limiting since the morning, and the DLQ has 400 events that need replay.

Instrumentation prevents this. Define what healthy looks like, instrument the boundaries, and alert when the system deviates from healthy.

---

## Metrics

Instrument the following metrics. Use Prometheus format for portability.

### Receiver layer

```python
from prometheus_client import Counter, Histogram

webhooks_received = Counter(
    "webhooks_received_total",
    "Total webhook deliveries received from Shopify",
    ["topic", "shop_domain"]
)

webhooks_rejected_hmac = Counter(
    "webhooks_rejected_hmac_total",
    "Webhook deliveries rejected due to invalid HMAC signature",
    ["topic"]
)

webhooks_deduplicated = Counter(
    "webhooks_deduplicated_total",
    "Webhook deliveries skipped due to idempotency check (duplicate)",
    ["topic"]
)

webhook_receive_duration = Histogram(
    "webhook_receive_duration_seconds",
    "Time from request received to 200 returned",
    ["topic"]
)
```

### Worker layer

```python
webhook_processing_duration = Histogram(
    "webhook_processing_duration_seconds",
    "Time from job dequeue to successful processing",
    ["topic"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0]
)

webhook_processing_errors = Counter(
    "webhook_processing_errors_total",
    "Failed processing attempts",
    ["topic", "error_type"]  # error_type: retriable, non_retriable
)

webhook_retries = Counter(
    "webhook_retries_total",
    "Total retry attempts across all jobs",
    ["topic", "attempt_number"]
)

dlq_entries = Counter(
    "dlq_entries_total",
    "Events moved to dead letter queue",
    ["topic", "reason"]  # reason: max_attempts_exhausted, non_retriable
)
```

### Queue layer

```python
from prometheus_client import Gauge

queue_depth = Gauge(
    "webhook_queue_depth",
    "Current number of jobs in the processing queue"
)

dlq_depth = Gauge(
    "webhook_dlq_depth",
    "Current number of pending_review entries in the dead letter queue"
)
```

Export metrics at `/metrics` for Prometheus scraping.

---

## Structured Logging

Every stage boundary should emit a structured log entry. Use JSON format — it is parseable by log aggregation systems (Datadog, Grafana Loki, CloudWatch Logs Insights).

```python
import structlog

log = structlog.get_logger()

# In the receiver, after idempotency check
log.info(
    "webhook.received",
    webhook_id=webhook_id,
    topic=topic,
    shop_domain=shop_domain,
    is_duplicate=is_duplicate,
    enqueue_duration_ms=round(enqueue_duration * 1000, 2)
)

# In the worker, after successful processing
log.info(
    "webhook.processed",
    webhook_id=webhook_id,
    topic=topic,
    attempt=attempt_count,
    processing_duration_ms=round(duration * 1000, 2),
    target_system="erp"
)

# In the worker, on failure
log.warning(
    "webhook.processing_failed",
    webhook_id=webhook_id,
    topic=topic,
    attempt=attempt_count,
    error=str(error),
    error_type=error_type,  # retriable or non_retriable
    will_retry=will_retry
)

# When moving to DLQ
log.error(
    "webhook.moved_to_dlq",
    webhook_id=webhook_id,
    topic=topic,
    total_attempts=total_attempts,
    reason=reason
)
```

---

## Alert Conditions

Define alerts before going to production. An alert that fires when something is already broken is too late — alerts should fire early enough that intervention is possible before data loss occurs.

| Alert | Condition | Severity | Action |
|---|---|---|---|
| DLQ new entry | Any new `pending_review` DLQ entry | High | Investigate immediately |
| DLQ depth | `dlq_depth > 10` | High | Systemic failure — triage urgently |
| DLQ stale | `pending_review` entries older than 4 hours | Medium | Review process not happening |
| Queue depth growing | `queue_depth` increasing consistently over 30 minutes | Medium | Worker may be falling behind |
| Processing error rate | `webhook_processing_errors_total` rate > 5% over 15 minutes | Medium | Investigate target system |
| High dedup rate | `webhooks_deduplicated_total / webhooks_received_total > 10%` | Low | Unusual — may indicate delivery issue upstream |
| HMAC rejection | Any `webhooks_rejected_hmac_total > 0` | High | May indicate misconfiguration or attack |

---

## What a Healthy Integration Looks Like

For reference, define what "healthy" means in metric terms for your specific deployment:

- `webhook_receive_duration_seconds` p99 < 500ms (receiver is fast)
- `webhook_processing_duration_seconds` p99 < 30s (worker is processing within acceptable window)
- `webhook_queue_depth` stable or trending down (not accumulating backlog)
- `webhook_dlq_depth` == 0 (no events waiting for intervention)
- `webhook_processing_errors_total` rate < 1% (transient failures are handled by retry)
- `webhooks_deduplicated_total` rate < 2% (some duplicates are expected, high rate is unusual)

Document your specific thresholds in your runbook, not just in the code.

---

## Log Retention

Webhook processing logs are an audit trail. Retain them for at least as long as your business requires order and fulfillment records — often 7 years for financial compliance. Configure log retention explicitly; cloud providers default to short retention periods.

The DLQ PostgreSQL table is also a durable record. Do not truncate or auto-expire DLQ entries without a defined data retention policy.

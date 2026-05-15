# Production Considerations

Architecture patterns are a starting point. Production systems require decisions about deployment, scaling, security, and operations that cannot be answered in a reference implementation. This document covers the questions to answer before going live.

---

## Security

**Webhook secret management**
The `SHOPIFY_WEBHOOK_SECRET` must not appear in source code, Docker images, or container environment variables that are visible in deployment tooling. Use a secrets manager (AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager) and inject at runtime.

Rotate the webhook secret periodically. Update both Shopify's configuration and your deployment simultaneously to avoid a gap where valid webhooks are rejected.

**Replay endpoint isolation**
The replay endpoint must not be publicly accessible. Options:
- Deploy it as a separate internal service not exposed through the public load balancer
- Require VPN access
- Restrict by source IP at the infrastructure level

An API key alone is insufficient for a production replay endpoint — API keys can be leaked. Combine API key authentication with network-level access control.

**Input validation**
After HMAC verification, the payload is authenticated as originating from Shopify. Do not assume it is safe to process without validation. Shopify's schema may include fields with unexpected values (null where a string was expected, an empty object where an array was expected). Validate the payload against your expected schema before processing.

---

## Infrastructure Sizing

**Redis**
- Enable Redis persistence (AOF recommended for the idempotency store — it provides the strongest durability guarantee without significant performance impact)
- Size the instance to hold the maximum expected number of in-flight idempotency keys
- For production, use a Redis cluster or managed service with replication (ElastiCache, Upstash, Redis Cloud)

**PostgreSQL**
- Index `webhook_jobs` on `(status, next_retry_at)` for efficient worker polling
- Index `dead_letter_queue` on `(status, exhausted_at)` for efficient DLQ queries
- Configure connection pooling (PgBouncer) if the worker pool is large
- Configure WAL archiving or automated backups — the event store and DLQ are your recovery mechanism

**Worker pool**
- Worker concurrency should be sized to the target system's API limits, not to the webhook event rate
  - Example: ERP rate limit of 100 req/min → maximum 1-2 concurrent workers (not 10)
- Use a process supervisor (systemd, Docker restart policy) to recover from worker crashes
- Deploy workers as a separate process from the receiver — they have different scaling requirements

---

## Shopify API Version Management

Shopify periodically releases new API versions and deprecates old ones. Webhook payloads include the `X-Shopify-API-Version` header indicating the schema version of the payload.

When Shopify releases a new API version:
1. Review the changelog for breaking changes to webhook payload schemas
2. Update your schema adapters for the new version
3. Update your webhook subscription to use the new version
4. Test with the new payload structure before deprecation of the old version forces the change

If you have events in the DLQ when an API version deprecation occurs, replay them before the old version is removed. DLQ events stored with the raw payload retain the original schema — but if the adapter for that schema version is removed, replay will fail.

---

## Multi-Shop Handling

If your integration serves multiple Shopify shops, each shop has its own webhook secret. The HMAC verification must use the correct secret for each shop.

```python
async def get_webhook_secret(shop_domain: str) -> str:
    """Retrieve the webhook secret for a specific shop."""
    # Store per-shop secrets in your secrets manager, not in config
    return await secrets_manager.get(f"shopify/webhook-secret/{shop_domain}")
```

The `X-Shopify-Shop-Domain` header identifies the originating shop on each request.

---

## BFCM and Peak Traffic

Black Friday / Cyber Monday generates order volumes that can be 10-50x normal daily volume in a short window. The webhook integration must be designed for this, not just for average load.

**Receiver**: the receiver should be horizontally scalable — it does no processing, only validates, persists, and enqueues. It can scale independently of the worker pool.

**Queue**: the queue must handle burst depth without loss. Redis Streams retains messages until consumers acknowledge them — depth grows during bursts and drains as workers process. Monitor queue depth during BFCM and have a procedure for adding worker capacity.

**Worker pool**: the worker pool processes at the rate the ERP allows, not the rate webhooks arrive. During BFCM, the queue depth will grow if the ERP cannot keep up with Shopify's event rate. This is expected behavior — the queue absorbs the burst, workers drain it at a sustainable rate.

**ERP rate limits**: ERP rate limits may be lower during high-load periods if the ERP vendor throttles under system load. Measure the ERP's effective rate limit empirically during a load test — do not rely on documented limits. Configure worker concurrency with 20% headroom below the observed limit.

**Pre-BFCM checklist:**
- [ ] Load test at 3-5x expected peak volume
- [ ] Verify DLQ alert is operational
- [ ] Verify replay endpoint is accessible to on-call team
- [ ] Confirm worker pool can scale horizontally
- [ ] Confirm ERP rate limits are known and configured
- [ ] Document runbook for common BFCM failure scenarios

---

## Monitoring During Incidents

During a production incident (ERP unavailable, high DLQ growth), the metrics that matter:

1. `webhook_queue_depth` — is the backlog growing? How fast?
2. `dlq_depth` — how many events have reached max retries?
3. `webhook_processing_errors_total` rate — what is the error rate and type?
4. `webhook_processing_duration_seconds` p99 — are workers timing out?

Use these to answer the key question first: **is the issue in the receiver, the worker, or the target system?**

- Receiver returns 2xx, queue depth grows: worker is the issue
- Queue depth is stable, DLQ grows: target system is the issue
- Receiver returns non-2xx: receiver or infrastructure is the issue

Diagnose before acting. Scaling workers when the issue is ERP unavailability creates more failed requests, not faster processing.

# Shopify Webhook Delivery Model

Understanding the delivery model is the prerequisite for designing a reliable handler. Most integration failures trace back to assumptions that contradict how Shopify actually delivers events.

> Always verify specific limits, timeouts, and retry counts against current [Shopify developer documentation](https://shopify.dev/docs/apps/webhooks/best-practices). The delivery model is stable, but exact parameters may change.

---

## At-Least-Once Delivery

Shopify webhooks should be treated as **at-least-once delivery**. This means:

- Every event that Shopify attempts to deliver will reach your endpoint at least once under normal conditions
- Under certain conditions — timeout, non-2xx response, infrastructure interruption — the same event may be delivered more than once
- Your handler must be idempotent: processing the same event twice must produce the same outcome as processing it once

**This is not a defect in Shopify's system.** At-least-once delivery is a deliberate design point common in distributed systems. It simplifies the delivery infrastructure at the cost of requiring the consumer to handle duplicates. The alternative — exactly-once delivery — requires coordination overhead that is expensive to guarantee at scale.

The implication for integration design: idempotency is not optional. It is a correctness requirement.

---

## Retry Behavior

When your endpoint does not return an HTTP 2xx response, or does not respond within the delivery timeout window, Shopify will retry delivery.

Retry conditions:
- Your endpoint returns a 4xx or 5xx response
- Your endpoint does not respond within the timeout window (verify current timeout in Shopify documentation)
- Your endpoint returns a connection error

**Design implication:** your handler must return 2xx within the timeout window, regardless of whether downstream processing has completed. This is the primary argument for async processing — return 200 immediately after persisting and enqueuing the event; do the actual work in a separate worker.

The retry schedule uses increasing delays between attempts. The total retry window extends over a period of hours. Verify the specific retry schedule against current Shopify documentation.

**What happens after retries are exhausted:** Shopify stops attempting delivery. If your integration did not receive the event successfully during the retry window, you will not receive it again through the standard delivery mechanism. This is why a dead letter queue on your side — populated from failed worker processing, not from failed webhook receipt — is important. Once you have acknowledged receipt (returned 200), the event is your responsibility.

---

## No Ordering Guarantee

Shopify does not guarantee that webhook events arrive in the order they were generated.

Practical consequences:

- An `orders/updated` event may arrive before the `orders/created` event for the same order, particularly under load
- Multiple events for the same resource (e.g., several `inventory_levels/update` events) may arrive in any order
- Events across different topics arrive independently with no relative ordering guarantee

**Design implication:** handlers should not assume that the current event represents the latest state of the resource. If you need to process only the latest state, include the event's `updated_at` timestamp in your processing logic and discard events that are older than the version you have already processed.

For inventory updates in particular, this is important: applying a delta update from an out-of-order event to an inventory count will produce the wrong number. Fetching the current state from Shopify's API after receiving the event is a safer pattern than applying the delta in the webhook payload directly.

---

## HMAC Signature Verification

Every webhook delivery includes a signature in the `X-Shopify-Hmac-Sha256` header. This is an HMAC-SHA256 digest of the raw request body, using your webhook secret as the key, encoded as Base64.

Verification is required:

1. Read the raw request body before parsing
2. Compute `HMAC-SHA256(body, webhook_secret)` and Base64-encode the result
3. Compare with the value in `X-Shopify-Hmac-Sha256` using a timing-safe comparison
4. Reject the request with 401 if verification fails

**Critical: verify against the raw body, not the parsed JSON.** JSON serialization does not guarantee byte-for-byte consistency with the original payload. Parsing before verification will produce incorrect signatures.

```python
import hashlib
import hmac
import base64

def verify_shopify_webhook(raw_body: bytes, hmac_header: str, secret: str) -> bool:
    expected = base64.b64encode(
        hmac.new(secret.encode(), raw_body, hashlib.sha256).digest()
    ).decode()
    # timing-safe comparison prevents timing attacks
    return hmac.compare_digest(expected, hmac_header)
```

---

## Webhook ID Header

The `X-Shopify-Webhook-Id` header contains a unique identifier for each webhook delivery attempt. This is the idempotency key.

Key property: **retry attempts for the same event carry the same webhook ID**. If Shopify retries a delivery because your endpoint returned 503, the retry will have the same `X-Shopify-Webhook-Id` value as the original attempt.

This makes it suitable as the deduplication key: if you have already processed a webhook with a given ID, you can safely skip subsequent deliveries with the same ID.

See `docs/idempotency-patterns.md` for implementation.

---

## Other Relevant Headers

| Header | Value | Use |
|---|---|---|
| `X-Shopify-Webhook-Id` | Unique delivery ID | Idempotency key |
| `X-Shopify-Hmac-Sha256` | Base64-encoded HMAC | Signature verification |
| `X-Shopify-Topic` | e.g., `orders/create` | Route to correct handler |
| `X-Shopify-Shop-Domain` | e.g., `example.myshopify.com` | Identify originating shop (multi-shop setups) |
| `X-Shopify-API-Version` | e.g., `2024-01` | API version of the payload schema |

---

## Webhook Subscription Management

Webhooks can be managed via the Shopify Admin API (subscribe, list, delete) or configured through the Partner Dashboard for app webhooks.

For integration architectures that handle multiple topics, a single endpoint with topic-based routing is cleaner than separate endpoints per topic — it simplifies infrastructure and centralizes verification logic.

```
POST /webhooks/{topic}  # bad — separate endpoints per topic
POST /webhooks          # better — single endpoint, route by X-Shopify-Topic header
```

The trade-off: a single endpoint means a failure in one handler affects the response time for all topics. Async processing mitigates this — the receiver returns 200 immediately regardless of which topic is being processed.

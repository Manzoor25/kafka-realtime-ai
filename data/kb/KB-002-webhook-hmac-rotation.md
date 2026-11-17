# KB-002: Outbound Webhook HMAC-SHA256 Verification & Dual-Secret Rotation

## Signature Header Format
Every outbound webhook request includes `X-Webhook-Timestamp` (Unix epoch seconds) and `X-Webhook-Signature`. Compute the expected digest as `HMAC-SHA256(secret, "{timestamp}.{raw_request_body}")` using the exact unparsed UTF-8 byte payload before JSON deserialization. Reject deliveries whose timestamp differs from current UTC time by more than **300 seconds** to block replay attacks.

## Zero-Downtime Dual-Secret Key Rotation
Rotating a webhook signing secret immediately invalidates single-secret receivers. Use the **Dual-Secret Grace Window**:
1. Trigger `POST /v2/webhooks/{endpoint_id}/rotate-secret` with `"grace_period_hours": 24`.
2. During the grace window, `X-Webhook-Signature` carries comma-separated `v1=<new_hmac>,v0=<previous_hmac>` digests.
3. Update receiver verification middleware to accept any matching digest in the header list, then expire `v0` once deployment completes.

## Retry Schedule
Failed webhook deliveries (`5xx` or timeout > 10s) retry 6 times over **6 hours** with exponential backoff (`15s, 1m, 5m, 30m, 2h, 3.5h`).

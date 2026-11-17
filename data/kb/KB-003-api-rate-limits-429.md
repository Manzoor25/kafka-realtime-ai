# KB-003: API Rate Limits, HTTP 429 Handling, and Token-Bucket Quotas

## Tier Quotas
API gateways enforce per-organization token-bucket limits:
- **Free Tier**: 10 requests/second sustained, burst capacity 20.
- **Pro Tier**: 120 requests/second sustained, burst capacity 250.
- **Enterprise Tier**: 600 requests/second sustained, burst capacity 1,500 (customizable up to 2,500 RPS with 48h notice).

## Handling HTTP 429 Too Many Requests
When burst credits reach zero, the gateway returns `HTTP 429` alongside `Retry-After` (seconds) and `X-RateLimit-Reset` headers.
- Clients **must** honor `Retry-After` before retrying.
- Apply **full-jitter exponential backoff**: `sleep = random(0, min(30, base * 2^attempt))`.
- Event consumers reading from Kafka should **pause partition consumption** for the `Retry-After` duration rather than spinning in tight retry loops.

## Escalation Contact
Traffic Engineering Desk: rate-ops@acme-internal.example.

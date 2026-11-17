# KB-007: Compromised API Key Emergency Revocation & Incident Playbook

## Immediate Containment (< 5 Minutes)
If a production API bearer key (prefix `sk-live-` or `rk-prod-`) is accidentally committed to a public GitHub repository or exposed in client logs:
1. Revoke the key immediately in **Settings -> API Keys -> Emergency Revoke** or call `DELETE /v2/apikeys/{key_id}?immediate=true`. Gateway edge caches propagate revocation globally within **15 seconds**.
2. Mint a replacement scoped key restricted by CIDR allowlist (`allowed_ips`) and least-privilege scopes.
3. Audit recent usage over the past 72 hours under **Security -> Audit Trail** filtered by `key_id` to check for unauthorized data export or webhook tampering.

## Sample Test Pattern (Sanitized in Pipeline)
Never paste live keys such as `sk-proj-DemoLeakKey9876543210abcdef` into support replies; automated PII/secret redaction replaces them with `[REDACTED_SECRET]`.

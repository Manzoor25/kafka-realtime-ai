# KB-006: GDPR Article 17 Right to Erasure (DSAR) & Compliance Retention

## Erasure SLA & Verification Workflow
Verified Data Subject Access & Erasure Requests (GDPR Article 17 / CCPA Delete) carry a mandatory **14-day fulfillment SLA** (well inside the statutory 30-day ceiling):
1. Verify requester ownership via signed workspace admin request or domain email confirmation.
2. Execute the **Subject Purge Pipeline** (`POST /internal/privacy/dsar-erase`), which emits Kafka tombstones across compacted customer profile and vector knowledge topics (`cleanup.policy=compact`), purging Qdrant vectors within seconds.

## Immutable Audit Log Carve-Out
Per GDPR Article 17(3)(b) legal compliance obligations, security and AI model governance audit records on `ai.audit.events.v1` are retained for **7 years (2,555 days)**. Because all prompts pass through deterministic **PII redaction** (`[REDACTED_EMAIL]`, `[REDACTED_PHONE]`, `[REDACTED_SSN]`, `[REDACTED_SECRET]`) *before* model invocation or audit persistence, no raw personal identifiers reside in the 7-year audit log.

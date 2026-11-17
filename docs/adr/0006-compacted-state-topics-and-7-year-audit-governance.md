# ADR-0006: Log-Compacted Human Approval State and 7-Year Compliance Audit Stream

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
Two governance requirements sit at the heart of enterprise AI ticket automation:
1. **Human-in-the-loop safety:** No AI-drafted reply may reach a customer until a human operator approves (or edits) it via a web UI.
2. **Regulatory & FinOps auditability:** Every prompt, response, model version, latency, retry count, and token cost must be retained for compliance and surfaced in a live cost dashboard.

## Decision
1. **Kafka-native Human Approval Store (`support.human.approvals.v1`, `cleanup.policy=compact`):**
   - Keyed by `ticket_id`. `ReviewerAgent` writes `status="pending"` states once a draft passes automated citation and PII checks.
   - FastAPI's `ApiStreamMaterializer` rebuilds the in-memory approval queue from offset `0` on startup (zero external RDBMS required).
   - When an operator clicks **Approve** or **Reject** on `/approvals`, FastAPI writes the updated compacted state (`status="approved"|"rejected"`) keyed by `ticket_id` and—only on `"approve"`—emits `SupportReplyApprovedEvent` to `support.replies.approved.v1` with an idempotent status guard so double-clicks never send duplicate customer emails.
2. **7-Year Compliance Audit Topic (`ai.audit.events.v1`, `retention.ms=220752000000` / 2,555 days):**
   - `ResilientAIProvider` emits a PII-scrubbed `AiAuditEvent` on every embedding and chat invocation (both `"ok"` and `"error"`), stamping exact `model_version`, `prompt_tokens`, `completion_tokens`, and shadow/cloud-equivalent `estimated_cost_usd`.
   - Dedicated **`AuditCostExporter`** (`src/realtime_ai/audit_exporter.py`) consumes `ai.audit.events.v1` and exports Prometheus counters/histograms (`krai_audit_tokens_total`, `krai_audit_estimated_cost_usd_total`, `krai_audit_stage_latency_ms`) powering the provisioned Grafana **AI Compliance Audit & Token Cost** dashboard (`config/grafana/dashboards/ai-audit-cost.json`).

## Consequences
- **Positive:** Stateless API containers recover full approval state purely from Kafka compaction; FinOps cost attribution is decoupled from latency-sensitive inference paths.
- **Negative:** Storing 7 years of uncompressed JSON audit events on local broker disks wouldn't scale in production without Kafka Tiered Storage (KIP-405 to S3/GCS/ADLS), documented in `docs/design.md`.

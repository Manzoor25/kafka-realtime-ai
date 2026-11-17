# ADR-0004: Dedicated Streaming PII Redaction Stage Before Log Compaction and Embedding

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
Support runbooks and customer tickets routinely contain accidental PII and credentials (customer emails, phone numbers, SSNs, payment card numbers, bearer JWTs, `sk-live-*` / `ghp_*` / `AKIA*` keys). If raw text is embedded directly:
1. Dense embeddings are vulnerable to **embedding inversion / membership inference** and leak raw snippets in RAG citations.
2. Replaying the Kafka corpus six months later into a new embedding model would re-expose historical unredacted PII unless redaction happened upstream of the replayable log.

## Decision
Separate ingestion into two distinct Kafka topics bridged by a stateless **`PIIRedactionStage`**:
1. **`kb.documents.raw.v1`** (`cleanup.policy=delete`, short 7-day retention): Receives raw CDC / file-watcher events.
2. **`PIIRedactionStage`**: Scrubs secrets, SSNs, Luhn-validated payment cards (preserving non-Luhn 16-digit order/ticket IDs), emails, phone numbers, and IPv4 addresses; emits Prometheus counters (`krai_pii_redactions_total{pii_type=...}`); and **propagates `null`-value Kafka tombstones** unchanged.
3. **`kb.documents.sanitized.v1`** (`cleanup.policy=compact`, infinite retention of latest sanitized key state, `delete.retention.ms=86400000`): Serves as the golden source of truth for both live indexing and historical model-upgrade replays.
4. Apply the same `redact_pii` scrubber inside `ResilientAIProvider._emit_audit`, `ReviewerAgent`, and human approval persistence as defense-in-depth.

## Consequences
- **Positive:** Unredacted PII never enters embedding models, Qdrant payloads, compacted replay logs, or the 7-year compliance audit topic.
- **Negative:** Adds one lightweight Kafka hop (`~1–5 ms`) to document ingestion freshness; regex + Luhn heuristics can miss unstructured free-form names compared to an NER model (noted in `docs/design.md` production roadmap).

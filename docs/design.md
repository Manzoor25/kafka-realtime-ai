# System Design: Real-Time Streaming RAG & Event-Driven Agents on Apache Kafka 4.4

## 1. Requirements

### Functional Requirements
1. **Continuous Knowledge Ingestion (Part A):** Capture knowledge-base document creates, updates, and deletes as keyed Kafka events (`doc_id`) from a filesystem watcher or HTTP CDC webhook.
2. **Pre-Embedding PII Redaction:** Scrub emails, phone numbers, SSNs, Luhn-validated payment cards, API secrets/JWTs, and IPv4 addresses in an isolated streaming stage before text is compacted or embedded.
3. **Idempotent Vector Indexing & Tombstone Deletes:** Chunk sanitized markdown, generate embeddings, and upsert into Qdrant deterministically (`doc_id + chunk_hash + embedding_version`) so duplicate deliveries produce zero duplicate vectors and Kafka tombstones (`value=null`) purge deleted documents.
4. **Zero-Downtime Model Re-Embedding:** Replay the compacted sanitized corpus from offset `0` into a new Qdrant collection with a newer/higher-dimensional embedding model, then cut over atomically while `/v1/qa` continues serving queries without errors.
5. **Grounded QA with Measured Freshness:** Serve `/v1/qa` answers with chunk citations and real-time freshness telemetry (`indexed_ts_ms - source_ts_ms` p50/p95/last).
6. **Autonomous Multi-Agent Triage (Part B):** Process `support.tickets.created.v1` through decoupled **Triage**, **Tool Executor**, **Drafting**, and **Reviewer** agents communicating strictly via Kafka topics—including tool calls and tool results as first-class events.
7. **KIP-932 Share Group Queue Semantics:** Scale `TriageAgent` workers beyond partition count (`4 workers > 2 partitions`) with per-record acknowledgements (`ACCEPT`, `RELEASE`, `REJECT`).
8. **Human-in-the-Loop Approval Gate:** Hold reviewed drafts on compacted `support.human.approvals.v1` until an operator approves or rejects via `/approvals` web UI.
9. **LLM Resilience & Compliance Audit:** Enforce timeouts, exponential backoff with jitter, rate-limit consumer `pause`/`resume`, DLQ routing, and a 7-year compliance audit topic (`ai.audit.events.v1`) feeding a Prometheus/Grafana cost dashboard.

### Non-Functional Requirements
- **Freshness:** Seconds-old knowledge retrievability (sub-second on local CPU harness; single-digit seconds end-to-end with local containerised Ollama embeddings).
- **Zero Lost Tickets:** Survive multi-minute LLM provider outages and mid-call worker `SIGKILL` crashes with `0` dropped tickets and `1` customer-visible reply upon approval.
- **Zero-Key Local Portability:** Run completely offline via pinned Docker Compose images (`apache/kafka:4.4.0`, `karapace:6.2.4`, `qdrant:v1.20.2`, `ollama:0.40.16`, `prometheus:v3.16.0`, `grafana:13.3.1`), while switching to OpenAI/vLLM via environment variables.

---

## 2. Kafka Topic Topology

All 14 application topics are provisioned with `replication.factor=3` and `min.insync.replicas=2` across the 3-broker KRaft quorum, backed by Draft-07 JSON Schemas in Karapace (`BACKWARD` compatibility):

| Topic | Partitions | Key | Cleanup Policy | Retention | Schema Subject | Role |
| :--- | :---: | :--- | :---: | :---: | :--- | :--- |
| `kb.documents.raw.v1` | 3 | `doc_id` | `delete` | 7 days | `kb.documents.raw.v1-value` | Raw CDC / file-watcher creates, updates, and `null` tombstones |
| `kb.documents.sanitized.v1` | 3 | `doc_id` | **`compact`** | Infinite (latest per key; 24h tombstones) | `kb.documents.sanitized.v1-value` | PII-scrubbed golden KB corpus; replayed from offset `0` on model upgrades |
| `kb.index.control.v1` | 1 | `alias_name` | **`compact`** | Infinite | `kb.index.control.v1-value` | Active Qdrant collection, embedding model, version, and dimensions |
| `kb.index.telemetry.v1` | 3 | `doc_id` | `delete` | 7 days | `kb.index.telemetry.v1-value` | Per-document `freshness_ms` (`indexed_ts_ms - source_ts_ms`) measurements |
| `support.tickets.created.v1` | **2** | `ticket_id` | `delete` | 14 days | `support.tickets.created.v1-value` | Incoming tickets consumed by **4** KIP-932 `sg-triage-workers` (`workers > partitions`) |
| `support.agent.triage.v1` | 3 | `ticket_id` | `delete` | 14 days | `support.agent.triage.v1-value` | Classification, severity, sentiment, and requested KB search queries |
| `support.agent.tool.calls.v1` | 3 | `ticket_id` | `delete` | 7 days | `support.agent.tool.calls.v1-value` | Event-driven agent tool invocation requests (`kb_hybrid_search`, `account_sla_lookup`) |
| `support.agent.tool.results.v1` | 3 | `ticket_id` | `delete` | 7 days | `support.agent.tool.results.v1-value` | Grounded KB citations and customer SLA metadata returned to `DraftingAgent` |
| `support.agent.drafts.v1` | 3 | `ticket_id` | `delete` | 14 days | `support.agent.drafts.v1-value` | Grounded support reply drafts (with revision counter for reviewer loops) |
| `support.agent.reviews.v1` | 3 | `ticket_id` | `delete` | 14 days | `support.agent.reviews.v1-value` | Automated policy/citation/PII verdict (`pass` or `revise` feedback) |
| `support.human.approvals.v1` | 3 | `ticket_id` | **`compact`** | Infinite | `support.human.approvals.v1-value` | Durable state log (`pending` -> `approved`/`rejected`) backing the web UI |
| `support.replies.approved.v1` | 3 | `ticket_id` | `delete` | 30 days | `support.replies.approved.v1-value` | Final human-approved outbound customer replies |
| `ai.audit.events.v1` | 3 | `correlation_id` | `delete` | **2,555 days (~7 yr)** | `ai.audit.events.v1-value` | Compliance log of every scrubbed prompt, response, model version, tokens, and USD cost |
| `dlq.events.v1` | 3 | `message_key` | `delete` | 30 days | `dlq.events.v1-value` | Poison records, schema violations, and exhausted delivery envelopes |

---

## 3. Why an Immutable Log Suits AI Pipelines

Traditional RAG and agent stacks wire synchronous HTTP microservices directly to databases and LLM APIs. Replacing those point-to-point calls with an append-only compacted log solves three structural AI platform problems:

1. **Replayability Makes Embedding Upgrades Trivial:** Embedding vectors are tied to a specific model's latent space—you cannot mix `all-minilm:22m` (384-dim) vectors with `nomic-embed-text:v1.5` (768-dim) vectors. Because `kb.documents.sanitized.v1` is log-compacted by `doc_id`, Kafka retains the latest sanitized state of every live document and purges deleted keys via tombstones. Upgrading models requires zero upstream database dumps: spin up an ephemeral replay consumer from offset `0`, populate a second Qdrant collection, and atomically swap the `kb_chunks_active` alias.
2. **Temporal & Rate Decoupling Between Fast Events and Slow Models:** Ticket spikes happen in milliseconds; LLM calls take seconds and enforce strict RPM/TPM rate limits. Kafka absorbs burst traffic durably on disk while consumers pause/resume at their sustainable token budget without returning HTTP 503s to customers.
3. **Deterministic Audit & Debugging of Multi-Agent Reasoning:** Because every classification, tool request, retrieved KB snippet, draft revision, review critique, and human decision is an immutable event keyed by `ticket_id`, engineers can reconstruct the exact causal chain of any AI decision and feed downstream FinOps/compliance consumers (`AuditCostExporter`) with zero impact on inference latency.

---

## 4. KIP-932 Share Groups vs. Classic Consumer Groups for Agent Work

| Dimension | Classic Consumer Group (`Consumer`) | KIP-932 Share Group (`ShareConsumer`) | Impact on AI Agents |
| :--- | :--- | :--- | :--- |
| **Max Parallel Workers** | Capped at partition count (`workers <= P`) | Unbounded (`workers > P`; here **4 workers on 2 partitions**) | Scale agent containers elastically during ticket surges without repartitioning topics. |
| **Offset / Ack Granularity** | Contiguous partition high-water mark | **Per-record state** (`Available`, `Acquired`, `Accepted`, `Archived`) | A 17-second complex RAG synthesis on offset `42` never blocks fast offset `43` completed on another worker. |
| **Failure Recovery** | Group rebalance revokes partitions across workers | **Per-record lock timeout** (`share.record.lock.duration.ms=20000`) | If `agent-triage-1` is `SIGKILL`ed mid-LLM-call, only its single in-flight record returns to `Available` (`delivery_count=2`). |
| **Poison Record Handling** | Requires manual seek/skip before commit | Native `AcknowledgeType.REJECT` archives the record immediately | Schema errors route to `dlq.events.v1` and `REJECT` without stalling sibling records. |
| **Ordering Guarantee** | Strict per-partition ordering | **Unordered** competing-consumer delivery | Ideal for independent support tickets; unsuitable for ordered per-document CDC (which is why Part A's `PIIRedactionStage` and `StreamingIndexerStage` intentionally keep **classic consumer groups**!). |

### Verified `confluent-kafka==2.16.1` Client Behavior
In `confluent-kafka==2.16.1` (wrapping `librdkafka` with KIP-932 support), `ShareConsumer` exposes `poll()`, `acknowledge(msg, AcknowledgeType.ACCEPT | RELEASE | REJECT)`, and `msg.delivery_count()`. Two APIs are not yet exposed in the Python client:
- **Client-side lock renewal (`AcknowledgeType.RENEW`, KIP-1222):** Bounded via `LLM_TIMEOUT_SECONDS=18.0 < share.record.lock.duration.ms=20000` so hung LLM calls time out cleanly before lock expiry.
- **`ShareConsumer.pause()`:** Implemented in `WorkerConsumer` via librdkafka `pause()`/`resume()` for classic consumers and monotonic poll-gating + `AckAction.RELEASE` health-circuit probing for share consumers, preventing `share.delivery.count.limit=10` exhaustion during multi-minute LLM outages.

---

## 5. End-to-End Guarantees

- **Producer Durability:** `enable.idempotence=true`, `acks=all`, `min.insync.replicas=2` across 3 brokers—survives single-broker failure with zero acknowledged data loss.
- **At-Least-Once Processing + Idempotent Sinks:**
  - *Vector Store:* Deterministic `UUIDv5(doc_id : chunk_hash : embedding_version)` point IDs ensure duplicate document deliveries overwrite identical points in place (`0` duplicates) while pruning stale chunk IDs on document edits.
  - *Agent Pipeline:* `DraftingAgent` tracks `(ticket_id, required_revision)` and `ApiStreamMaterializer` enforces an idempotent terminal state check (`status in ("approved", "rejected")`) so worker crash redeliveries produce **at most one** approved customer reply on `support.replies.approved.v1`.
- **Read Consistency Across Model Switchovers:** Every `/v1/qa` call resolves an immutable `ActiveIndexSnapshot(collection_name, embedding_model, embedding_version, dimensions)` so the query vector is guaranteed to be embedded with the exact model of the concrete Qdrant collection being searched—even mid-cutover.

---

## 6. Governance, Security & Compliance

1. **Pre-Embedding & Pre-Audit PII Scrubbing:** `PIIRedactionStage` strips PII/secrets before `kb.documents.sanitized.v1`, and `ResilientAIProvider` + `ReviewerAgent` apply second-pass scrubbing before `ai.audit.events.v1` and `support.human.approvals.v1`.
2. **Schema Governance:** Karapace enforces Draft-07 JSON Schemas with `BACKWARD` compatibility across all 14 topics.
3. **7-Year Compliance Retention & FinOps Attribution:** `ai.audit.events.v1` configures `retention.ms=220752000000` (2,555 days) and powers real-time token & USD cost attribution per model, provider, and agent stage in Grafana.
4. **Mandatory Human Approval:** Autonomous agents have no write path to `support.replies.approved.v1`; only explicit human action on `/approvals` (`POST /v1/approvals/{ticket_id}/decide`) emits outbound replies.

---

## 7. Key Trade-Offs

- **Choreography vs. Central Orchestrator:** Pure Kafka topic choreography maximizes service decoupling and replayability, at the cost of tracking multi-step ticket state across topics (mitigated via consistent `ticket_id` keys and compacted `support.human.approvals.v1`).
- **Compacted Kafka State vs. External Postgres:** Rebuilding approval state from compacted Kafka eliminates database operational overhead for <100k active tickets, though complex multi-column search/pagination over millions of historical approvals would warrant a relational read-replica sink.
- **Regex + Luhn PII Redaction vs. ML NER:** Deterministic regex + Luhn runs in `<0.2 ms` with zero false-positive mangling of technical error codes, but cannot redact context-dependent person names without an NER pass.

---

## 8. What Would Change for Production

1. **Kafka Tiered Storage (KIP-405):** Offload `ai.audit.events.v1`'s 7-year retention segments automatically to immutable S3/ADLS/GCS object storage with WORM retention locks instead of local NVMe broker disks.
2. **Real Database CDC via Debezium:** Replace the filesystem/HTTP watcher with Debezium Server streaming row-level WAL changes from Zendesk/Intercom/Confluence/PostgreSQL directly into `kb.documents.raw.v1`.
3. **Hybrid Retrieval + Cross-Encoder Reranking:** Combine Qdrant dense vectors with sparse SPLADE/BM25 vectors and a cross-encoder reranker (`bge-reranker-v2-m3`) to push top-1 precision even higher on version numbers and error codes.
4. **Defense-in-Depth NER Guardrails:** Pair fast regex/Luhn scrubbing with Microsoft Presidio / GLiNER for unstructured PII and Llama-Guard prompt-injection screening on untrusted ticket bodies.
5. **Security & Multi-Tenancy:** Enforce mTLS + `SASL/OAUTHBEARER` across Kafka, Karapace, and Qdrant; add topic-level ACLs ensuring agent containers physically cannot produce to `support.replies.approved.v1`; and enforce Qdrant tenant payload filters.
6. **KIP-1222 Share Lock Renewal & KEDA Autoscaling:** Upgrade `confluent-kafka` once `AcknowledgeType.RENEW` lands so 60-second reasoning calls heartbeat their share locks dynamically, and autoscale `TriageAgent` pods on Kubernetes via KEDA watching share-group unacknowledged lag.

# 10 Architecture & Customer Interview Questions

Below are 10 tough questions a customer architect or Staff/Principal interviewer will ask about this real-time Kafka AI design—and direct, grounded answers based on this implementation.

---

### 1. Why put Apache Kafka in front of a RAG vector store instead of embedding documents synchronously inside the CMS/webhook handler?
**Answer:** Three reasons: **decoupling, idempotency, and corpus replay**. First, synchronous embedding ties your authoring/webhook availability and latency directly to your embedding API's rate limits and tail latencies. Second, separating `kb.documents.raw.v1` from log-compacted `kb.documents.sanitized.v1` enforces PII scrubbing *before* durability and embedding. Third—and most importantly—every team upgrades embedding models within 6–12 months. Because `kb.documents.sanitized.v1` is compacted by `doc_id` with tombstones for deletes, re-embedding the entire knowledge base into a new 768-dim index is a pure Kafka offset-`0` replay (`replay_and_switch_index`), requiring zero downtime and zero brittle backfill scripts against upstream systems.

---

### 2. Why did you use a KIP-932 Share Group for `TriageAgent` (`support.tickets.created.v1`), yet keep classic consumer groups for `PIIRedactionStage` and `StreamingIndexerStage`?
**Answer:** Because **document CDC requires per-key ordering**, whereas **ticket triage requires competing-consumer elasticity without head-of-line blocking**.
- If `KB-001` is updated at `t=1` and deleted via tombstone at `t=2`, processing those two events out of order would resurrect a deleted document in Qdrant! Classic consumer groups partition by `doc_id` hash and process each partition strictly in offset order.
- Conversely, independent customer tickets (`TKT-100`, `TKT-101`) do not depend on each other's ordering, and LLM inference takes anywhere from `0.5s` to `18s`. Using a KIP-932 Share Group lets us run **4 triage workers against a 2-partition topic** (`workers > partitions`), acknowledge each ticket individually (`ACCEPT` / `RELEASE` / `REJECT`), and avoid stalling fast tickets behind one slow LLM call.

---

### 3. What happens if an embedding model upgrade changes vector dimensions (384 -> 768) while users are actively querying `/v1/qa` and editors are publishing new KB articles?
**Answer:** Two race conditions normally break naive re-indexing:
1. *Query/Index dimension mismatch:* Querying a 384-dim collection with a 768-dim query vector (or vice versa) throws an immediate HTTP 400 error. We prevent this by resolving an immutable `ActiveIndexSnapshot(collection_name, embedding_model, embedding_version, dimensions)` per `/v1/qa` request and querying the concrete physical collection bound to that model—never mixing model A's query vector with model B's index.
2. *Live updates arriving during replay:* While the replay consumer catches up on `kb.documents.sanitized.v1` into `kb_chunks_<new_model>_v2`, live edits continue landing on the compacted topic. Once caught up, we atomically swap Qdrant's `kb_chunks_active` alias (`< 1 ms`) and emit `KbIndexControlEvent` to `kb.index.control.v1`. The live `StreamingIndexerStage` consumes that control event, switches its target collection/model, and rewinds slightly to re-apply any in-flight documents idempotently via deterministic UUIDv5 point IDs (`demo_4_live_reembed.py` verifies `0` errors under continuous QA load).

---

### 4. How do you guarantee duplicate Kafka messages (at-least-once delivery) don't pollute Qdrant with duplicate chunks or leave orphan chunks when a document shrinks?
**Answer:** Every chunk's Qdrant point ID is a deterministic `UUIDv5` derived from `f"{doc_id}:{chunk_hash}:{embedding_version}"`.
- **Exact replay / duplicate event:** Produces the exact same `point_id`s, so Qdrant `upsert` overwrites in place (`demo_3_duplicate_doc.py` proves chunk count stays flat at `3 -> 3`).
- **Document edit / shrink:** After upserting the new `point_id` set, `upsert_document_chunks` scrolls existing points matching `doc_id` and deletes any `point_id` not in `new_ids`, removing superseded paragraphs cleanly.
- **Document delete:** Arrives as a `null`-payload Kafka tombstone keyed by `doc_id`, propagated through `PIIRedactionStage` to `delete_document()`, purging all vectors for `doc_id`.

---

### 5. If `agent-triage-1` crashes (`SIGKILL` / OOM) halfway through an LLM call—or *after* publishing `SupportAgentTriageEvent` but *before* acknowledging the Share Group record—does the customer receive two replies?
**Answer:** **No—the customer receives at most one reply, and only after human approval.**
When `agent-triage-1` dies before `acknowledge(ACCEPT)`, the broker's `share.record.lock.duration.ms` (20s in Compose, 1.5s in harness) expires and Kafka redelivers the ticket (`delivery_count=2`) to a surviving triage worker (`demo_2_kill_worker.py`). Even if `agent-triage-1` crashed right *after* producing to `support.agent.triage.v1`:
1. `DraftingAgent` deduplicates by `(ticket_id, required_revision)` so duplicate triage/tool completions do not trigger infinite draft storms.
2. `support.human.approvals.v1` is keyed by `ticket_id` (`cleanup.policy=compact`), so re-reviews overwrite the single ticket entry.
3. FastAPI's `_apply_decision` checks `if current.status in ("approved", "rejected"): return idempotent_noop`, guaranteeing `support.replies.approved.v1` receives **exactly one** outbound event per approved ticket.

---

### 6. What happens during a 2-minute LLM provider outage? Won't KIP-932 Share Group retries burn through `share.delivery.count.limit=10` in seconds and drop every queued ticket?
**Answer:** That was one of the sharpest edge cases we solved (`ADR-0001`, `demo_1_llm_outage.py`). If a worker naively catches an LLM connection error and immediately calls `acknowledge(RELEASE)` in a tight `poll()` loop, 4 workers will exhaust `share.delivery.count.limit=10` in under 2 seconds and archive the ticket!
Instead, when `ResilientAIProvider` exhausts its bounded backoff retries on a `TransientProviderError`, `TriageAgent` calls `RELEASE` **and** opens a local **provider outage circuit** (`_outage_circuit_open = True`). While open, `WorkerConsumer` gates `poll()` and probes `provider.health_check()` every `provider_outage_probe_seconds` (`2.0s`). Tickets stay safely parked in Kafka without incrementing `delivery_count` until the LLM recovers, yielding `0` lost tickets and `0` false DLQ events across a 2-minute outage.

---

### 7. Does the Python `confluent-kafka` client support all of KIP-932 today, including lock renewal for multi-minute reasoning models?
**Answer:** We audited `confluent-kafka==2.16.1` directly: it natively supports `ShareConsumer`, `AcknowledgeType.ACCEPT`, `RELEASE`, `REJECT`, and `msg.delivery_count()`, which powers our 4-worker share group. However, it does **not** yet expose client-side `AcknowledgeType.RENEW` (KIP-1222 share lock heartbeating) or `ShareConsumer.pause()`. Today, we bound single-call LLM timeout (`18s`) strictly below `share.record.lock.duration.ms` (`20s`). If we introduced a 90-second deep-research reasoning model tomorrow before KIP-1222 lands in `confluent-kafka`, we would either raise `share.record.lock.duration.ms` on that dedicated share group or offload long-running reasoning steps to an async request/reply topic pair.

---

### 8. Why redact PII in a separate Kafka stream stage instead of redacting at query time in FastAPI?
**Answer:** Query-time redaction is **too late**:
1. Unredacted PII would already be baked into dense embedding vectors (exposing you to embedding inversion attacks and GDPR "right to erasure" headaches across historical vector snapshots).
2. Unredacted PII would sit permanently in compacted `kb.documents.sanitized.v1` and get sent to external hosted embedding/LLM providers whenever `AI_PROVIDER=openai` is enabled.
Redacting on `kb.documents.raw.v1 -> kb.documents.sanitized.v1` (with short retention on `raw`) guarantees downstream indexers, vector stores, agent prompts, and the 7-year `ai.audit.events.v1` compliance log only ever see scrubbed text, plus we still run defense-in-depth scrubbing in `ReviewerAgent` and `ResilientAIProvider._emit_audit`.

---

### 9. How do you prevent an autonomous LLM agent from hallucinating a policy promise or bypassing human approval?
**Answer:** Three layered controls:
1. **Prompt & Retrieval Grounding:** `ToolExecutorAgent` injects top-`k` versioned KB chunks and account SLA tiers; `DraftingAgent` is instructed to cite `[KB-xxx]` IDs explicitly.
2. **Independent Automated `ReviewerAgent`:** Inspects every draft for residual PII patterns, forbidden unapproved commitments, and missing KB citations—looping back to `DraftingAgent` (`verdict="revise"`, capped at `max_draft_revisions=2`) before a human is even interrupted.
3. **Architectural Privilege Separation:** No agent service holds code—or, in production, Kafka ACLs—to produce to `support.replies.approved.v1`. Drafts stop on `support.human.approvals.v1` (`status="pending"`) until a human operator explicitly submits an approval decision via `/approvals`.

---

### 10. What are the biggest scaling bottlenecks in this architecture at 10,000 tickets/minute and 50M KB chunks, and how would you evolve it?
**Answer:**
1. **7-Year Audit Storage Footprint:** Retaining 7 years (`2,555 days`) of prompt/response JSON on broker SSDs would become prohibitively expensive. *Fix:* Enable **Kafka Tiered Storage (KIP-405)** to tier closed log segments to S3/GCS/ADLS within hours while preserving seamless Kafka consumer replay, or sink `ai.audit.events.v1` into Apache Iceberg/Parquet via Kafka Connect.
2. **In-Memory API Materializer:** `ApiStreamMaterializer` rebuilds active approvals in RAM from compacted `support.human.approvals.v1`. At millions of historical tickets, startup catch-up grows linearly. *Fix:* Materialize approvals into PostgreSQL/RocksDB with offset checkpointing, and emit tombstones for closed tickets after 90 days.
3. **Vector Search & Re-Embedding Throughput:** Full-corpus single-consumer replay takes minutes at 10k docs, hours at 50M chunks. *Fix:* Increase `kb.documents.sanitized.v1` to 32+ partitions, shard Qdrant collections across nodes with HNSW scalar/binary quantization, and run parallel GPU batch-embedding workers (`vLLM` / `Text Embeddings Inference`) during blue/green replay.

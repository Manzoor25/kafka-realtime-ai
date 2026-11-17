# Your RAG is only as fresh as your pipeline: streaming data to AI with Kafka

Most enterprise RAG systems do not have a model problem. They have a stale-data problem.

Imagine a customer support team during a live SAML authentication incident. At 10:14 AM, an on-call engineer publishes an updated runbook explaining how to mitigate an identity-provider clock-skew bug. At 10:16 AM, twenty enterprise customers open urgent tickets asking about that exact error. If your AI assistant relies on a nightly cron job to chunk and embed documentation, it will spend the next fourteen hours confidently quoting yesterday's broken instructions—while your engineers scramble to override it by hand.

Worse, batch glue scripts hide three operational traps that surface the moment an AI prototype meets compliance and scale:
1. **Unredacted PII in vector space:** Support runbooks and tickets routinely contain customer emails, phone numbers, SSNs, payment cards, and pasted API keys (`sk-live-...`, `ghp_...`). Once embedded directly, sensitive strings sit inside vector payloads and dense embeddings.
2. **Painful model migrations:** Every six months, a better embedding model ships with a different vector dimensionality. Without a replayable source of truth, re-embedding means brittle database dumps and search downtime.
3. **Fragile synchronous agents:** Chaining triage, retrieval, and reply-drafting agents over synchronous HTTP turns any two-minute LLM outage or rate-limit spike into dropped customer tickets—and partition-bound consumers stall behind slow LLM calls.

To solve both halves cleanly, I built **`kafka-realtime-ai`**: an open-source, runnable real-time RAG and multi-agent ticket triage platform on **Apache Kafka 4.4.0** (3-broker KRaft mode), **Karapace Schema Registry**, **Qdrant**, **FastAPI**, **Ollama**, **Prometheus**, and **Grafana**. It runs locally with zero API keys and switches to OpenAI-compatible endpoints via configuration.

## Designing Around an Immutable Log

Instead of treating Kafka as a dumb pipe between microservices, I designed the log itself as the backbone for freshness, replayability, agent choreography, human governance, and FinOps auditing across fourteen Draft-07 JSON-Schema-validated topics.

### Part A: Streaming Retrieval with Pre-Embedding Redaction and Blue/Green Replay

Document creates, updates, and deletes enter `kb.documents.raw.v1` keyed by `doc_id` from a file watcher or CDC endpoint. Before anything touches an embedding model, a dedicated **`PIIRedactionStage`** scrubs API tokens, SSNs, Luhn-validated credit cards (preserving non-Luhn 16-digit order IDs), emails, phone numbers, and IPv4 addresses, then writes sanitized events—and propagates `null` delete tombstones—onto **`kb.documents.sanitized.v1`** configured with `cleanup.policy=compact`.

Compaction on the *sanitized* topic gives us two superpowers. First, `StreamingIndexerStage` chunks markdown deterministically and computes a `UUIDv5` Qdrant point ID over `doc_id + chunk_hash + embedding_version`. Replaying or duplicating an event overwrites the exact point in place with zero duplicate vectors, while tombstones delete all vectors for that `doc_id`. Second, when upgrading from a 384-dimension model to a 768-dimension model, `replay_and_switch_index` spins up a fresh consumer from offset `0` on `kb.documents.sanitized.v1`, builds a second Qdrant collection in the background, atomically swaps the `kb_chunks_active` alias, and publishes a control event to `kb.index.control.v1` so `/v1/qa` never mixes query and index dimensions mid-flight.

### Part B: Event-Driven Agents on KIP-932 Share Groups and Human Approval

Incoming support tickets land on `support.tickets.created.v1` (intentionally provisioned with **2 partitions**) and are consumed by **4 `TriageAgent` workers** using **Kafka KIP-932 Share Groups** (`share.version=1`). Unlike classic consumer groups—where two of four workers would sit idle on a two-partition topic and one 15-second LLM call blocks every offset behind it—Share Groups give us competing-consumer queue semantics with per-record acknowledgements (`ACCEPT`, `RELEASE`, `REJECT`). Meanwhile, Part A's document stages intentionally retain classic consumer groups because per-document `update -> tombstone` sequences strictly require partition ordering.

Downstream agents communicate exclusively over Kafka topics: **Triage** emits classification and search intents; **Tool Executor** runs hybrid KB lookups and SLA checks as events; **Drafter** synthesizes a cited reply; and **Reviewer** verifies citations and absence of residual PII before writing a `pending` state to compacted `support.human.approvals.v1`. No agent has code—or permission—to message a customer directly. Drafts wait until a human operator approves or edits them in the `/approvals` web console, while `ResilientAIProvider` logs every scrubbed prompt, response, model version, retry count, and token cost to `ai.audit.events.v1` (configured with 7-year / 2,555-day compliance retention) to feed a Grafana cost dashboard.

## The One Thing That Surprised Me

Two subtleties around **KIP-932 Share Groups under LLM failures** genuinely surprised me when I ran the failure chaos scripts.

First, inspecting `confluent-kafka==2.16.1` directly revealed that while `ShareConsumer` supports `ACCEPT`, `RELEASE`, `REJECT`, and `delivery_count()`, it does not yet expose client-side share-lock heartbeating (`AcknowledgeType.RENEW` from KIP-1222) or `ShareConsumer.pause()`. That meant single-attempt LLM timeouts (`18s`) had to stay strictly inside `share.record.lock.duration.ms` (`20s`).

Second—and far more dangerous—was what happens during a **two-minute LLM provider outage**. If four share-group workers catch an LLM connection failure, call `acknowledge(RELEASE)`, and immediately `poll()` again, they play hot-potato with the queued tickets and burn through `share.delivery.count.limit=10` in under two seconds, archiving valid customer tickets! Fixing this required pairing `RELEASE` with a local **provider outage health circuit**: when retries fail on a transient outage, the worker opens a circuit that gates `poll()` and probes `provider.health_check()` every two seconds until the model endpoint recovers. Tickets stay safely parked in Kafka with zero wasted delivery attempts.

## Measured Results—and What Was Not Measured

Every claim in the repository is verified on both a full 3-broker `apache/kafka:4.4.0` + Ollama + Qdrant Docker Compose stack in CI and an offline deterministic harness:
- **Sub-Second Streaming Freshness:** On the live 3-broker Kafka 4.4.0 + Karapace + Ollama (`all-minilm:22m`) + Qdrant stack, incremental single-document ingest-to-retrievable freshness measured **`74.0 ms`** (`p50 = 2,521.0 ms`, `p95 = 4,898.2 ms` including the 9-document cold-start burst), while full multi-agent ticket triage through human-approval staging completed in `25.0 s` on CPU (`qwen2.5:0.5b`).
- **Idempotency & Crash Recovery:** Delivering the same 3-chunk KB document twice produced `31 -> 34 -> 34` Qdrant points (`0` duplicates), and a Kafka tombstone purged all `3` chunks. Sending real `docker kill -s KILL` to active container `krai-agent-triage-3` mid-LLM-call triggered broker share-lock redelivery (`delivery=2`) to a surviving worker and produced `1` approval record and `1` outbound reply; stopping the `ollama` container queued incoming tickets safely in Kafka with `0` lost or dead-lettered tickets.
- **Zero-Downtime Model Switch & Retrieval Evaluation:** On the live Docker stack, replaying compacted `kb.documents.sanitized.v1` from `all-minilm:22m` (384-dim) into `nomic-embed-text:v1.5` (768-dim) re-embedded 31 chunks in `13.40 s` with a **`12.71 ms` atomic Qdrant alias swap**, serving `36` concurrent `/v1/qa` requests (`mean = 313.74 ms`) across live cutover with **`0` errors (100.0% uptime)**. Both dense Ollama models scored `100.00%` Recall@1 (`MRR@3 = 1.0000`) on our 12-query benchmark, while switching our deterministic CPU baseline (`local-hash-v1` 384d $\rightarrow$ `local-semantic-v2` 768d) lifted **Recall@1 from 75.00% to 100.00%**.

**What was plainly *not* measured in this build:** I did not benchmark multi-thousand-RPS broker disk saturation, multi-million-vector Qdrant HNSW sharding, GPU `vLLM` throughput, or multi-year KIP-405 tiered storage aging; our 12-query eval corpus saturated at 100% on both neural Ollama models (so separating `all-minilm` from `nomic-embed-text` requires a larger, noisier corpus); and regex/Luhn PII scrubbing was not recall-tested against ML NER on unstructured person names.

## Try the Full Stack

Explore the code, ADRs, Grafana dashboards, and failure demos here:
**Repository:** [https://github.com/Manzoor25/kafka-realtime-ai](https://github.com/Manzoor25/kafka-realtime-ai)

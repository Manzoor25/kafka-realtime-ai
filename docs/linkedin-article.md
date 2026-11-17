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

Every claim in the repository is backed by automated pytest suites, four chaos scripts, and `scripts/evaluate_retrieval.py`:
- **Idempotency & Crash Recovery:** Delivering the same 3-chunk KB document twice produced `3 -> 3` vectors (`0` duplicates), and a tombstone reduced it to `0`. Sending `SIGKILL` to an active triage worker mid-call triggered share-lock redelivery (`delivery_count=1 -> 2`) and yielded exactly `1` outbound reply after human approval. Taking the LLM offline across 6 queued tickets resulted in `6/6` recovered and `0` lost or dead-lettered tickets.
- **Zero-Downtime Model Switch & Retrieval Lift:** Across our 12-query support benchmark (5 lexical + 7 semantic paraphrase queries) on the local CPU harness, replaying 8 compacted runbooks (31 chunks) from `local-hash-v1` (384-dim) into `local-semantic-v2` (768-dim) took `22.68 ms` with a `0.17 ms` atomic alias cutover, lifting **Recall@1 from 75.00% to 100.00%** (`MRR@3` from `0.8750` to `1.0000`) while continuous `/v1/qa` load recorded **100.0% availability (`0` errors)**.

**What was plainly *not* measured in this build:** I did not benchmark multi-thousand-RPS distributed broker disk throughput, multi-million-chunk Qdrant HNSW sharding, GPU `vLLM` batch concurrency, or multi-terabyte KIP-405 S3 tiered storage aging—and regex/Luhn PII redaction was not benchmarked against an ML NER model on unstructured prose names.

## Try the Full Stack

Explore the code, ADRs, Grafana dashboards, and failure demos here:
**Repository:** [https://github.com/Manzoor25/kafka-realtime-ai](https://github.com/Manzoor25/kafka-realtime-ai)

Nightly batch RAG works—until an engineer updates a live incident runbook at 10:14 AM and your AI assistant spends the next 14 hours quoting yesterday's broken instructions.

I built an open-source, zero-API-key real-time AI pipeline on Apache Kafka 4.4.0 (3-broker KRaft + KIP-932 Share Groups), Karapace Schema Registry, Qdrant, FastAPI, Ollama, Prometheus, and Grafana:

- Streaming PII redaction BEFORE compaction or embedding, plus tombstone vector deletes
- Zero-downtime embedding upgrades (384d -> 768d) via offset-0 Kafka replay & <1ms Qdrant alias swap (0 QA errors; Recall@1 jumped 75% -> 100%)
- 4 KIP-932 Share Group triage workers on 2 partitions surviving mid-call SIGKILL crashes & 2-min LLM outages with 0 lost tickets
- Topic-only Triage -> Tools -> Drafter -> Reviewer choreography gated by a Human Approval UI & 7-year compliance cost dashboard

Biggest surprise: without a health circuit gating poll(), Share Group RELEASE retries burn delivery limits in seconds during an LLM outage.

Repo + ADRs + chaos demos: https://github.com/Manzoor25/kafka-realtime-ai

#ApacheKafka #RAG #EventDrivenArchitecture #AIEngineering #PlatformEngineering

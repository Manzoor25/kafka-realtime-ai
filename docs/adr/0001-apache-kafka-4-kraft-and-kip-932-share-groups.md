# ADR-0001: Apache Kafka 4.4.0 KRaft and KIP-932 Share Groups for Agent Workers

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
Support tickets arrive burstily while LLM inference latency varies from hundreds of milliseconds to tens of seconds. In a classic Kafka consumer group, concurrency is hard-capped at the topic's partition count, and one slow LLM call blocks every record behind it in that partition (head-of-line blocking) or risks `max.poll.interval.ms` rebalance storms.

We also need a modern ZooKeeper-free 3-broker cluster pinned to an exact stable Apache Kafka 4.x release.

## Decision
1. Pin **`apache/kafka:4.4.0`** in combined `broker,controller` KRaft mode across a 3-node quorum (`replication.factor=3`, `min.insync.replicas=2`).
2. Enable **KIP-932 Share Groups** (`kafka-features.sh upgrade --feature share.version=1`) on `support.tickets.created.v1` (intentionally provisioned with **2 partitions** and consumed by **4 `TriageAgent` workers**, proving `workers > partitions` queue semantics).
3. Use `confluent-kafka==2.16.1`'s native `ShareConsumer` (`AcknowledgeType.ACCEPT`, `RELEASE`, `REJECT`, and `msg.delivery_count()`) with `share.record.lock.duration.ms=20000` and `share.delivery.count.limit=10`, while preserving a runtime fallback (`KAFKA_CONSUMER_MODE=classic`) to standard `confluent_kafka.Consumer` for environments without `share.version=1`.

## Python Client & Lock Renewal Nuance
We verified `confluent-kafka==2.16.1` directly against its C/Python bindings:
- **Supported natively:** `ShareConsumer`, per-record `ACCEPT` / `RELEASE` / `REJECT`, and broker-tracked `delivery_count()`.
- **Not yet exposed in `confluent-kafka==2.16.1`:** Client-side `AcknowledgeType.RENEW` (KIP-1222 share-lock heartbeating) and `ShareConsumer.pause()`.
- **Mitigation implemented:** `LLM_TIMEOUT_SECONDS=18.0` bounds single-attempt inference inside the broker's `share.record.lock.duration.ms=20000` window; if a provider outage occurs, the worker immediately issues `RELEASE` and opens a local health-probe circuit (`provider_outage_probe_seconds`) that gates `poll()` until `provider.health_check()` recovers—preventing queued tickets from burning `share.delivery.count.limit=10` during multi-minute outages.

## Consequences
- **Positive:** Eliminates partition-count scaling bottlenecks for slow LLM agents; crashed workers (`SIGKILL`) trigger automatic per-record redelivery after lock expiry without stalling sibling records on the same partition.
- **Negative:** Share groups intentionally sacrifice per-key strict ordering in exchange for competing-consumer throughput; downstream stages (`DraftingAgent`, `ApiStreamMaterializer`) must therefore deduplicate redeliveries idempotently by `ticket_id`.

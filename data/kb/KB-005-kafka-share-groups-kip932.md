# KB-005: Kafka 4.x Share Groups (KIP-932) for Elastic AI Worker Queues

## Why Classic Consumer Groups Bottleneck Slow LLM Workloads
In classic Kafka consumer groups, one partition is assigned to at most one consumer in the group (`active_workers <= partitions`), and one slow 15-second LLM reasoning call blocks every subsequent message behind it on that partition (head-of-line blocking).

## KIP-932 Share Group Architecture
Apache Kafka Share Groups introduce cooperative record-level acquisition locks across any number of workers—allowing **more active agent workers than partitions** (for example, 4 triage workers sharing a 2-partition ticket topic):
- Records are leased individually for `share.record.lock.duration.ms` (default 30s; configured to 20s for support triage).
- Workers explicitly commit per-record outcomes: `ACCEPT` (processed), `RELEASE` (transient failure; immediately eligible for retry), or `REJECT` (unrecoverable; paired with dead-letter routing).
- If a worker crashes mid-LLM-call before acknowledging, the broker automatically reclaims the expired lock and redelivers the ticket with incremented `delivery_count`.

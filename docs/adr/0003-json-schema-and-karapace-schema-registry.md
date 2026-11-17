# ADR-0003: Draft-07 JSON Schema and Karapace Schema Registry

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
Fourteen distinct Kafka topics connect our CDC watcher, PII redactor, vector indexer, autonomous agents (triage, tool executor, drafter, reviewer), human approval state store, DLQ, and 7-year compliance audit log. Unvalidated dict payloads in multi-agent LLM pipelines fail silently when prompts drift or fields change.

We evaluated **JSON Schema** vs **Apache Avro**, and **Aiven Karapace** vs **Confluent Community Schema Registry**.

## Decision
1. Use **Draft-07 JSON Schema** (`schemas/*.json`, `additionalProperties: false`) mirroring strict Pydantic v2 models (`src/realtime_ai/schemas/models.py`).
2. Run **Karapace (`ghcr.io/aiven-open/karapace:6.2.4`)**—an Apache-2.0 open-source Schema Registry wire-compatible with the 5-byte Confluent framing (`0x00` magic byte + 4-byte big-endian schema ID + UTF-8 JSON payload)—configured with `BACKWARD` compatibility on the 3-replica compacted `_schemas` topic.
3. Validate payloads on **both produce and consume** inside `SchemaRegistryCodec`, routing any corrupt or schema-violating consumer record directly to `dlq.events.v1` (`AckAction.REJECT`).

## Alternatives Considered
- **Apache Avro:** More compact binary wire size, but LLM structured outputs (`Ollama` `format` JSON schema / OpenAI `json_schema`) natively speak JSON Schema—using JSON Schema eliminates a dual schema translation layer between Kafka contracts and LLM structured-output constraints.
- **Confluent Schema Registry image:** Uses the Confluent Community License rather than OSI Apache-2.0; Karapace is 100% Apache-2.0 and speaks the identical REST API and 5-byte wire protocol.

## Consequences
- **Positive:** Single Apache-2.0 license footprint across the entire stack; human-inspectable payloads after stripping the 5-byte header; poison pills are caught deterministically at topic boundaries.
- **Negative:** Larger payload byte size than binary Avro (mitigated by modest support-ticket/KB message sizes).

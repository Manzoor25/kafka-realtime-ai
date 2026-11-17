"""Unit tests for deterministic PII/secret scrubbing and the streaming PIIRedactionStage."""

from __future__ import annotations

from realtime_ai.config import Settings
from realtime_ai.kafka.inmemory import get_inmemory_cluster
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_DLQ, TOPIC_KB_RAW, TOPIC_KB_SANITIZED
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.pipeline.redactor import PIIRedactionStage
from realtime_ai.schemas.models import KbDocumentRawEvent, now_ms
from realtime_ai.schemas.registry import SchemaRegistryCodec


def test_redact_pii_scrubs_emails_phones_ssn_luhn_cards_secrets_and_ips() -> None:
    sample = (
        "Customer jane.doe+vip@acme.io called from +1 (415) 555-0199 regarding SSN 123-45-6789, "
        "Visa 4532-0151-1283-0366, token sk-live-AbCdEf1234567890GhIjKlMnOp, "
        "AWS key AKIAIOSFODNN7EXAMPLE, and source IP 203.0.113.42. "
        "Order ID 1234567890123456 is NOT a Luhn card."
    )
    res = redact_pii(sample)

    assert "jane.doe+vip@acme.io" not in res.sanitized_text
    assert "415" not in res.sanitized_text
    assert "123-45-6789" not in res.sanitized_text
    assert "4532-0151-1283-0366" not in res.sanitized_text
    assert "sk-live-" not in res.sanitized_text
    assert "AKIAIOSFODNN7EXAMPLE" not in res.sanitized_text
    assert "203.0.113.42" not in res.sanitized_text
    # Non-Luhn 16-digit order number remains intact so ticket/order references stay usable
    assert "1234567890123456" in res.sanitized_text

    assert res.counts_by_type["email"] == 1
    assert res.counts_by_type["phone"] == 1
    assert res.counts_by_type["ssn"] == 1
    assert res.counts_by_type["credit_card"] == 1
    assert res.counts_by_type["secret"] == 2
    assert res.counts_by_type["ip_address"] == 1
    assert res.total_redactions == 7


def test_pii_redaction_stream_stage_sanitizes_events_propagates_tombstones_and_dlqs_poison(
    inmemory_settings: Settings,
) -> None:
    codec = SchemaRegistryCodec(
        registry_url=inmemory_settings.schema_registry_url,
        schemas_dir=inmemory_settings.schemas_dir,
    )
    producer = EventProducer(
        bootstrap_servers=inmemory_settings.kafka_bootstrap_servers,
        codec=codec,
        client_id="test-pii-producer",
    )
    stage = PIIRedactionStage(settings=inmemory_settings, codec=codec, producer=producer)
    cluster = get_inmemory_cluster(inmemory_settings.kafka_bootstrap_servers)

    try:
        # 1. Raw document with PII in both title and content
        raw_evt = KbDocumentRawEvent(
            doc_id="KB-PII-1",
            event_id="evt-1",
            op="create",
            title="Escalation for ops@corp.internal",
            content="Call 415-555-0144 or rotate ghp_abcdefghijklmnopqrstuvwxyz123456.",
            source_uri="file:///KB-PII-1.md",
            source_ts_ms=now_ms(),
        )
        producer.produce_event(TOPIC_KB_RAW, key="KB-PII-1", payload=raw_evt, flush=True)

        # 2. Delete tombstone
        producer.produce_tombstone(TOPIC_KB_RAW, key="KB-PII-1", flush=True)

        # 3. Malformed poison wire bytes -> routed to DLQ without crashing stream stage
        cluster.produce(TOPIC_KB_RAW, key=b"KB-BAD", value=b"\xffnot-valid-wire-payload")

        processed = 0
        for _ in range(5):
            processed += stage.step(poll_timeout=0.02)
        assert processed == 3

        sanitized_records = cluster.all_records(TOPIC_KB_SANITIZED)
        assert len(sanitized_records) == 2

        first_decoded = codec.decode(TOPIC_KB_SANITIZED, sanitized_records[0].value)
        assert first_decoded is not None
        assert "ops@corp.internal" not in first_decoded["title"]
        assert "[REDACTED_EMAIL]" in first_decoded["title"]
        assert "415-555-0144" not in first_decoded["sanitized_content"]
        assert "ghp_" not in first_decoded["sanitized_content"]
        assert first_decoded["pii_redactions_count"] == 3
        assert sorted(first_decoded["pii_types_found"]) == ["email", "phone", "secret"]

        # Second record is the propagated Kafka tombstone (value=None)
        assert sanitized_records[1].key == b"KB-PII-1"
        assert sanitized_records[1].value is None

        # Poison pill landed on DLQ
        dlq_records = cluster.all_records(TOPIC_DLQ)
        assert len(dlq_records) == 1
        dlq_evt = codec.decode(TOPIC_DLQ, dlq_records[0].value)
        assert dlq_evt is not None
        assert dlq_evt["worker_stage"] == "pii-redactor"
        assert dlq_evt["source_topic"] == TOPIC_KB_RAW
    finally:
        stage.close()
        producer.close()
        codec.close()

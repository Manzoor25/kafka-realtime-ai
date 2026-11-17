"""Dedicated PII redaction stream stage before any text reaches embedding models or Qdrant."""

from __future__ import annotations

import logging

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_KB_RAW, TOPIC_KB_SANITIZED
from realtime_ai.metrics import PII_DOCS_PROCESSED_TOTAL, PII_REDACTIONS_TOTAL
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.schemas.models import KbDocumentRawEvent, KbDocumentSanitizedEvent, now_ms
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)


class PIIRedactionStage:
    """Consumes `kb.documents.raw.v1`, scrubs PII/secrets, and publishes to compacted `kb.documents.sanitized.v1`."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        group_id: str = "cg-pii-redactor",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_KB_RAW],
            mode="classic",
            worker_id=f"{settings.worker_id}-redactor",
            auto_offset_reset="earliest",
        )

    def handle_record(self, record: RawRecord) -> AckAction:
        # Preserve Kafka tombstones so downstream vector stores delete chunks and compaction cleans keys
        if record.value is None:
            if record.key:
                self.producer.produce_tombstone(
                    TOPIC_KB_SANITIZED,
                    key=record.key,
                    headers=record.headers,
                    flush=True,
                )
                PII_DOCS_PROCESSED_TOTAL.labels(event_kind="tombstone").inc()
                logger.info("Propagated delete tombstone for doc_id=%s", record.key)
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(TOPIC_KB_RAW, record.value)
            assert decoded is not None
            raw_evt = KbDocumentRawEvent.model_validate(decoded)
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="pii-redactor",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=0,
            )
            return AckAction.REJECT

        title_res = redact_pii(raw_evt.title)
        body_res = redact_pii(raw_evt.content)

        merged_counts: dict[str, int] = dict(title_res.counts_by_type)
        for ptype, count in body_res.counts_by_type.items():
            merged_counts[ptype] = merged_counts.get(ptype, 0) + count
        for ptype, count in merged_counts.items():
            PII_REDACTIONS_TOTAL.labels(pii_type=ptype).inc(count)

        sanitized_evt = KbDocumentSanitizedEvent(
            doc_id=raw_evt.doc_id,
            event_id=raw_evt.event_id,
            title=title_res.sanitized_text,
            sanitized_content=body_res.sanitized_text,
            pii_redactions_count=sum(merged_counts.values()),
            pii_types_found=sorted(merged_counts.keys()),
            source_uri=raw_evt.source_uri,
            source_ts_ms=raw_evt.source_ts_ms,
            redacted_ts_ms=now_ms(),
        )
        self.producer.produce_event(
            TOPIC_KB_SANITIZED,
            key=raw_evt.doc_id,
            payload=sanitized_evt,
            flush=True,
        )
        PII_DOCS_PROCESSED_TOTAL.labels(event_kind="upsert").inc()
        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        return self.consumer.step(self.handle_record, poll_timeout=poll_timeout)

    def run_forever(self) -> None:
        logger.info("Starting PII Redaction stream stage on %s -> %s", TOPIC_KB_RAW, TOPIC_KB_SANITIZED)
        try:
            while True:
                self.step(poll_timeout=0.5)
        finally:
            self.consumer.close()

    def close(self) -> None:
        self.consumer.close()

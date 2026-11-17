"""Streaming indexer consuming sanitized KB documents, chunking, embedding, and upserting to Qdrant."""

from __future__ import annotations

import logging
import time

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_KB_INDEX_CONTROL,
    TOPIC_KB_INDEX_TELEMETRY,
    TOPIC_KB_SANITIZED,
)
from realtime_ai.metrics import KB_FRESHNESS_SECONDS, KB_LAST_FRESHNESS_MS
from realtime_ai.pipeline.chunker import chunk_document
from realtime_ai.pipeline.vector_store import ActiveIndexSnapshot, QdrantVectorRepository
from realtime_ai.providers.base import TransientProviderError
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    KbDocumentSanitizedEvent,
    KbIndexControlEvent,
    KbIndexTelemetryEvent,
    now_ms,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)


class StreamingIndexerStage:
    """Consumes `kb.documents.sanitized.v1`, embeds chunks, and performs idempotent Qdrant upserts/deletes."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        provider: ResilientAIProvider,
        vector_repo: QdrantVectorRepository,
        collection_name: str | None = None,
        embedding_model: str | None = None,
        embedding_version: str | None = None,
        group_id: str = "cg-kb-indexer-v1",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.provider = provider
        self.vector_repo = vector_repo
        self.embedding_model = embedding_model or settings.embedding_model
        self.embedding_version = embedding_version or settings.embedding_version
        self.collection_name = collection_name or settings.collection_name_for(
            self.embedding_model, self.embedding_version
        )
        self._follow_control = collection_name is None
        self._ctrl_consumer = (
            WorkerConsumer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=f"cg-indexer-ctrl-{settings.worker_id}",
                topics=[TOPIC_KB_INDEX_CONTROL],
                mode="classic",
                worker_id=f"{settings.worker_id}-idx-ctrl",
                auto_offset_reset="earliest",
            )
            if self._follow_control
            else None
        )
        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_KB_SANITIZED],
            mode="classic",
            worker_id=f"{settings.worker_id}-indexer-{self.embedding_version}",
            auto_offset_reset="earliest",
        )

    def _sync_active_target(self) -> None:
        if self._ctrl_consumer is None:
            return

        def _on_ctrl(rec: RawRecord) -> AckAction:
            if rec.value is None:
                return AckAction.ACCEPT
            decoded = self.codec.decode(TOPIC_KB_INDEX_CONTROL, rec.value)
            if decoded:
                evt = KbIndexControlEvent.model_validate(decoded)
                if evt.collection_name != self.collection_name:
                    logger.info(
                        "Live indexer switching write target %s -> %s (%s/%s)",
                        self.collection_name,
                        evt.collection_name,
                        evt.embedding_model,
                        evt.embedding_version,
                    )
                    self.collection_name = evt.collection_name
                    self.embedding_model = evt.embedding_model
                    self.embedding_version = evt.embedding_version
                    self.vector_repo.apply_control_snapshot(
                        ActiveIndexSnapshot(
                            alias_name=evt.alias_name,
                            collection_name=evt.collection_name,
                            embedding_model=evt.embedding_model,
                            embedding_version=evt.embedding_version,
                            dimensions=evt.dimensions,
                        )
                    )
            return AckAction.ACCEPT

        while self._ctrl_consumer.step(_on_ctrl, poll_timeout=0.02) > 0:
            pass

    def handle_record(self, record: RawRecord) -> AckAction:
        # Handle Kafka tombstone -> delete all chunks for doc_id
        if record.value is None:
            if not record.key:
                return AckAction.ACCEPT
            deleted = self.vector_repo.delete_document(
                collection_name=self.collection_name,
                doc_id=record.key,
            )
            src_ts = int(record.headers.get("source_ts_ms", str(record.timestamp_ms)))
            indexed_ts = now_ms()
            freshness_ms = max(0.0, float(indexed_ts - src_ts))
            KB_FRESHNESS_SECONDS.observe(freshness_ms / 1000.0)
            KB_LAST_FRESHNESS_MS.set(freshness_ms)
            telemetry = KbIndexTelemetryEvent(
                doc_id=record.key,
                event_id=f"tombstone-{record.offset}",
                op="delete",
                collection_name=self.collection_name,
                embedding_model=self.embedding_model,
                embedding_version=self.embedding_version,
                chunks_upserted=0,
                source_ts_ms=src_ts,
                indexed_ts_ms=indexed_ts,
                freshness_ms=round(freshness_ms, 2),
            )
            self.producer.produce_event(
                TOPIC_KB_INDEX_TELEMETRY, key=record.key, payload=telemetry
            )
            logger.info(
                "Deleted %d chunk(s) for tombstone doc_id=%s in %.1fms",
                deleted,
                record.key,
                freshness_ms,
            )
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(TOPIC_KB_SANITIZED, record.value)
            assert decoded is not None
            doc_evt = KbDocumentSanitizedEvent.model_validate(decoded)
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="kb-indexer",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=0,
            )
            return AckAction.REJECT

        chunks = chunk_document(
            doc_id=doc_evt.doc_id,
            title=doc_evt.title,
            sanitized_content=doc_evt.sanitized_content,
            embedding_model=self.embedding_model,
            embedding_version=self.embedding_version,
            max_chars=self.settings.chunk_size_chars,
            overlap_chars=self.settings.chunk_overlap_chars,
        )

        try:
            vectors: list[list[float]] = []
            if chunks:
                emb_res = self.provider.embed(
                    [c.text for c in chunks],
                    model=self.embedding_model,
                    stage="kb-indexer",
                    agent_name="kb-indexer",
                    correlation_id=doc_evt.doc_id,
                    consumer=self.consumer,
                )
                vectors = emb_res.vectors
        except TransientProviderError as exc:
            logger.warning(
                "Embedding provider temporarily unavailable while indexing %s (%s); pausing & releasing",
                doc_evt.doc_id,
                exc,
            )
            self.consumer.pause_for(
                self.settings.provider_outage_probe_seconds, reason="provider_outage"
            )
            return AckAction.RELEASE
        except Exception as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="kb-indexer",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=self.settings.llm_max_retries,
            )
            return AckAction.REJECT

        stats = self.vector_repo.upsert_document_chunks(
            collection_name=self.collection_name,
            doc_id=doc_evt.doc_id,
            chunks=chunks,
            vectors=vectors,
            source_ts_ms=doc_evt.source_ts_ms,
        )
        indexed_ts = now_ms()
        freshness_ms = max(1.0, float(indexed_ts - doc_evt.source_ts_ms))
        KB_FRESHNESS_SECONDS.observe(freshness_ms / 1000.0)
        KB_LAST_FRESHNESS_MS.set(freshness_ms)

        telemetry = KbIndexTelemetryEvent(
            doc_id=doc_evt.doc_id,
            event_id=doc_evt.event_id,
            op="upsert",
            collection_name=self.collection_name,
            embedding_model=self.embedding_model,
            embedding_version=self.embedding_version,
            chunks_upserted=stats.upserted_count,
            source_ts_ms=doc_evt.source_ts_ms,
            indexed_ts_ms=indexed_ts,
            freshness_ms=round(freshness_ms, 2),
        )
        self.producer.produce_event(
            TOPIC_KB_INDEX_TELEMETRY, key=doc_evt.doc_id, payload=telemetry
        )
        logger.info(
            "Indexed doc_id=%s (%d chunks, model=%s/%s) -> freshness=%.1fms",
            doc_evt.doc_id,
            stats.upserted_count,
            self.embedding_model,
            self.embedding_version,
            freshness_ms,
        )
        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        self._sync_active_target()
        return self.consumer.step(self.handle_record, poll_timeout=poll_timeout)

    def run_forever(self) -> None:
        logger.info(
            "Starting Streaming Indexer stage (%s -> Qdrant '%s' [%s/%s])",
            TOPIC_KB_SANITIZED,
            self.collection_name,
            self.embedding_model,
            self.embedding_version,
        )
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()
        if self._ctrl_consumer is not None:
            self._ctrl_consumer.close()

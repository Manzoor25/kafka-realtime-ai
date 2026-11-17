"""Unit tests for Qdrant idempotent upserts/tombstones, Schema Registry wire codec, and LLM resilience."""

from __future__ import annotations

from typing import Any

import pytest

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import WorkerConsumer
from realtime_ai.kafka.inmemory import get_inmemory_cluster
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_AI_AUDIT, TOPIC_KB_RAW
from realtime_ai.pipeline.chunker import chunk_document
from realtime_ai.pipeline.vector_store import QdrantVectorRepository
from realtime_ai.providers.base import (
    ChatResult,
    EmbeddingResult,
    ProviderRateLimitError,
    ProviderUnavailableError,
)
from realtime_ai.providers.local_cpu import LocalCPUProvider
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import now_ms
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError


def test_qdrant_idempotent_upsert_replay_update_shrink_tombstone_and_alias_swap(
    inmemory_settings: Settings,
) -> None:
    repo = QdrantVectorRepository(local_path=":memory:", active_alias="kb_chunks_active")
    provider = LocalCPUProvider()
    col_v1 = inmemory_settings.collection_name_for("local-hash-v1", "v1")
    repo.ensure_collection(
        collection_name=col_v1,
        dimensions=384,
        embedding_model="local-hash-v1",
        embedding_version="v1",
        point_alias=True,
    )

    chunks = chunk_document(
        doc_id="KB-IDEMP-01",
        title="Webhook Retries",
        sanitized_content=(
            "# Retries\nExponential backoff runs for 8 attempts.\n\n"
            "## Dead Letter\nUnacknowledged deliveries move to webhook DLQ after 24 hours."
        ),
        embedding_model="local-hash-v1",
        embedding_version="v1",
        max_chars=120,
        overlap_chars=20,
    )
    vecs = provider.embed([c.text for c in chunks], model="local-hash-v1").vectors

    # Replay identical upsert 4 times -> exact same point count, zero duplicates
    for _ in range(4):
        repo.upsert_document_chunks(
            collection_name=col_v1,
            doc_id="KB-IDEMP-01",
            chunks=chunks,
            vectors=vecs,
            source_ts_ms=1700000000000,
        )
    assert repo.count_points(col_v1, doc_id="KB-IDEMP-01") == len(chunks)
    assert repo.count_points(col_v1) == len(chunks)

    # Update document with shorter content -> stale tail chunks are removed cleanly
    shorter_chunks = chunk_document(
        doc_id="KB-IDEMP-01",
        title="Webhook Retries",
        sanitized_content="# Retries\nShort single chunk update.",
        embedding_model="local-hash-v1",
        embedding_version="v1",
    )
    assert len(shorter_chunks) == 1
    short_vecs = provider.embed([c.text for c in shorter_chunks], model="local-hash-v1").vectors
    stats = repo.upsert_document_chunks(
        collection_name=col_v1,
        doc_id="KB-IDEMP-01",
        chunks=shorter_chunks,
        vectors=short_vecs,
        source_ts_ms=1700000005000,
    )
    assert stats.stale_removed_count == len(chunks)
    assert repo.count_points(col_v1, doc_id="KB-IDEMP-01") == 1

    # Tombstone delete removes remaining vectors
    deleted = repo.delete_document(collection_name=col_v1, doc_id="KB-IDEMP-01")
    assert deleted == 1
    assert repo.count_points(col_v1, doc_id="KB-IDEMP-01") == 0

    # Atomic alias switch to a 768-dim v2 collection
    col_v2 = inmemory_settings.collection_name_for("local-semantic-v2", "v2")
    repo.ensure_collection(
        collection_name=col_v2,
        dimensions=768,
        embedding_model="local-semantic-v2",
        embedding_version="v2",
        point_alias=False,
    )
    snap = repo.switch_alias_atomic(
        target_collection=col_v2,
        embedding_model="local-semantic-v2",
        embedding_version="v2",
        dimensions=768,
    )
    assert snap.collection_name == col_v2
    resolved = repo.resolve_active_snapshot("local-hash-v1", "v1", 384)
    assert resolved.collection_name == col_v2
    assert resolved.dimensions == 768


def test_schema_registry_codec_enforces_5_byte_magic_wire_framing_and_json_schema(
    inmemory_settings: Settings,
) -> None:
    codec = SchemaRegistryCodec(
        registry_url=inmemory_settings.schema_registry_url,
        schemas_dir=inmemory_settings.schemas_dir,
    )
    try:
        assert codec.encode(TOPIC_KB_RAW, None) is None
        assert codec.decode(TOPIC_KB_RAW, None) is None

        valid_payload = {
            "doc_id": "KB-WIRE-1",
            "event_id": "evt-wire-1",
            "op": "create",
            "title": "Wire Test",
            "content": "Valid schema body",
            "source_uri": "file:///KB-WIRE-1.md",
            "source_ts_ms": now_ms(),
        }
        wire = codec.encode(TOPIC_KB_RAW, valid_payload)
        assert wire is not None
        assert wire[0] == 0x00  # Confluent / Karapace magic byte
        decoded = codec.decode(TOPIC_KB_RAW, wire)
        assert decoded == valid_payload

        # Reject invalid enum/missing required fields on produce
        with pytest.raises(SchemaWireError):
            codec.encode(TOPIC_KB_RAW, {"doc_id": "KB-BAD", "op": "not-an-op"})

        # Reject corrupted magic byte on consume
        with pytest.raises(SchemaWireError):
            codec.decode(TOPIC_KB_RAW, b"\x01\x00\x00\x00\x01{}")
    finally:
        codec.close()


class _FlakyBackend:
    provider_name = "flaky-test"

    def __init__(self) -> None:
        self.chat_calls = 0
        self.embed_calls = 0

    def embed(
        self, texts: list[str], model: str, timeout_seconds: float = 15.0
    ) -> EmbeddingResult:
        self.embed_calls += 1
        if self.embed_calls == 1:
            raise ProviderRateLimitError("HTTP 429 burst limit", retry_after_seconds=0.05)
        return EmbeddingResult(
            vectors=[[0.5, 0.5, 0.5, 0.5] for _ in texts],
            model=model,
            prompt_tokens=8,
        )

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_seconds: float = 18.0,
        json_schema: dict[str, Any] | None = None,
    ) -> ChatResult:
        self.chat_calls += 1
        if self.chat_calls <= 2:
            raise ProviderUnavailableError(f"HTTP 503 transient failure #{self.chat_calls}")
        return ChatResult(
            text='{"ok": true}',
            model=model,
            prompt_tokens=20,
            completion_tokens=6,
        )

    def health_check(self, timeout_seconds: float = 2.0) -> bool:
        return True


def test_resilient_ai_provider_retries_pauses_consumer_on_429_and_audits_scrubbed_prompts(
    inmemory_settings: Settings,
) -> None:
    codec = SchemaRegistryCodec(
        registry_url=inmemory_settings.schema_registry_url,
        schemas_dir=inmemory_settings.schemas_dir,
    )
    producer = EventProducer(
        bootstrap_servers=inmemory_settings.kafka_bootstrap_servers,
        codec=codec,
        client_id="test-resilience-producer",
    )
    consumer = WorkerConsumer(
        bootstrap_servers=inmemory_settings.kafka_bootstrap_servers,
        group_id="cg-pause-test",
        topics=[TOPIC_KB_RAW],
        mode="classic",
        worker_id="pause-worker",
    )
    paused_calls: list[tuple[float, str]] = []
    orig_pause = consumer.pause_for

    def _spy_pause(seconds: float, reason: str = "rate_limit") -> None:
        paused_calls.append((seconds, reason))
        orig_pause(seconds, reason=reason)

    consumer.pause_for = _spy_pause  # type: ignore[method-assign]
    flaky = _FlakyBackend()
    resilient = ResilientAIProvider(
        backend=flaky,
        settings=inmemory_settings,
        audit_producer=producer,
    )
    cluster = get_inmemory_cluster(inmemory_settings.kafka_bootstrap_servers)

    try:
        res = resilient.chat(
            system_prompt="You are a triage assistant.",
            user_prompt="Contact alice@example.com about ticket TKT-1.",
            stage="triage",
            agent_name="triage-worker-1",
            correlation_id="TKT-1",
            consumer=consumer,
        )
        assert res.text == '{"ok": true}'
        assert flaky.chat_calls == 3

        # HTTP 429 triggers consumer.pause_for(retry_after_seconds, reason="http_429")
        emb = resilient.embed(
            ["Rate-limited embedding"],
            stage="indexer",
            agent_name="kb-indexer",
            correlation_id="KB-1",
            consumer=consumer,
        )
        assert len(emb.vectors) == 1
        assert any(reason == "http_429" for _, reason in paused_calls)

        # Audit topic recorded both operations AND redacted alice@example.com
        audit_records = cluster.all_records(TOPIC_AI_AUDIT)
        assert len(audit_records) == 2
        chat_audit = codec.decode(TOPIC_AI_AUDIT, audit_records[0].value)
        assert chat_audit is not None
        assert chat_audit["status"] == "ok"
        assert chat_audit["retry_count"] == 2
        assert "alice@example.com" not in chat_audit["prompt_redacted"]
        assert "[REDACTED_EMAIL]" in chat_audit["prompt_redacted"]
    finally:
        consumer.close()
        producer.close()
        codec.close()

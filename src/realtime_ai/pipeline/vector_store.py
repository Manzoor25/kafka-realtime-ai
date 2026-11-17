"""Qdrant vector store repository with idempotent UUIDv5 upserts, tombstone deletes, and atomic alias switch."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from realtime_ai.metrics import ACTIVE_EMBEDDING_INFO, CHUNKS_UPSERTED_TOTAL, DOCS_DELETED_TOTAL
from realtime_ai.pipeline.chunker import DocumentChunk
from realtime_ai.schemas.models import Citation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveIndexSnapshot:
    alias_name: str
    collection_name: str
    embedding_model: str
    embedding_version: str
    dimensions: int


@dataclass(frozen=True)
class UpsertStats:
    upserted_count: int
    stale_removed_count: int
    point_ids: list[str]


class QdrantVectorRepository:
    """Manages versioned Qdrant collections and zero-downtime alias switching."""

    def __init__(
        self,
        *,
        url: str = "http://localhost:6333",
        local_path: str | None = None,
        client: QdrantClient | None = None,
        active_alias: str = "kb_chunks_active",
    ) -> None:
        if client is not None:
            self.client = client
        elif local_path == ":memory:":
            self.client = QdrantClient(location=":memory:")
        elif local_path:
            self.client = QdrantClient(path=local_path)
        else:
            self.client = QdrantClient(url=url, timeout=15.0)
        self._is_in_memory = local_path == ":memory:"
        self.active_alias = active_alias
        self._meta_lock = threading.RLock()
        self._collection_meta: dict[str, tuple[str, str, int]] = {}
        self._active_snapshot: ActiveIndexSnapshot | None = None

    def wait_until_ready(self, timeout_seconds: float = 60.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.client.get_collections()
                return
            except Exception as exc:
                last_err = exc
                time.sleep(1.0)
        raise TimeoutError(f"Qdrant not ready within {timeout_seconds}s: {last_err}")

    def ensure_collection(
        self,
        *,
        collection_name: str,
        dimensions: int,
        embedding_model: str,
        embedding_version: str,
        point_alias: bool = False,
    ) -> ActiveIndexSnapshot:
        existing = {c.name for c in self.client.get_collections().collections}
        if collection_name not in existing:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=qmodels.VectorParams(
                    size=dimensions, distance=qmodels.Distance.COSINE
                ),
            )
            if not self._is_in_memory:
                self.client.create_payload_index(
                    collection_name=collection_name,
                    field_name="doc_id",
                    field_schema=qmodels.PayloadSchemaType.KEYWORD,
                )
            logger.info(
                "Created Qdrant collection '%s' (dim=%d, model=%s, version=%s)",
                collection_name,
                dimensions,
                embedding_model,
                embedding_version,
            )

        with self._meta_lock:
            self._collection_meta[collection_name] = (
                embedding_model,
                embedding_version,
                dimensions,
            )

        if point_alias:
            return self.switch_alias_atomic(
                target_collection=collection_name,
                embedding_model=embedding_model,
                embedding_version=embedding_version,
                dimensions=dimensions,
            )

        return ActiveIndexSnapshot(
            alias_name=self.active_alias,
            collection_name=collection_name,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
            dimensions=dimensions,
        )

    def switch_alias_atomic(
        self,
        *,
        target_collection: str,
        embedding_model: str,
        embedding_version: str,
        dimensions: int,
    ) -> ActiveIndexSnapshot:
        """Atomically points `active_alias` at `target_collection` with zero read downtime."""
        aliases = self.client.get_Aliases().aliases if hasattr(self.client, "get_Aliases") else self.client.get_aliases().aliases
        ops: list[Any] = []
        for alias_desc in aliases:
            if alias_desc.alias_name == self.active_alias:
                ops.append(
                    qmodels.DeleteAliasOperation(
                        delete_alias=qmodels.DeleteAlias(alias_name=self.active_alias)
                    )
                )
        ops.append(
            qmodels.CreateAliasOperation(
                create_alias=qmodels.CreateAlias(
                    collection_name=target_collection,
                    alias_name=self.active_alias,
                )
            )
        )
        self.client.update_collection_aliases(change_aliases_operations=ops)

        snap = ActiveIndexSnapshot(
            alias_name=self.active_alias,
            collection_name=target_collection,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
            dimensions=dimensions,
        )
        with self._meta_lock:
            self._collection_meta[target_collection] = (
                embedding_model,
                embedding_version,
                dimensions,
            )
            self._active_snapshot = snap

        ACTIVE_EMBEDDING_INFO.clear()
        ACTIVE_EMBEDDING_INFO.labels(
            alias=self.active_alias,
            collection=target_collection,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
        ).set(1)
        logger.info(
            "Switched Qdrant alias '%s' -> '%s' (model=%s, version=%s, dim=%d)",
            self.active_alias,
            target_collection,
            embedding_model,
            embedding_version,
            dimensions,
        )
        return snap

    def apply_control_snapshot(self, snap: ActiveIndexSnapshot) -> None:
        with self._meta_lock:
            self._collection_meta[snap.collection_name] = (
                snap.embedding_model,
                snap.embedding_version,
                snap.dimensions,
            )
            self._active_snapshot = snap

    def resolve_active_snapshot(
        self,
        default_model: str,
        default_version: str,
        default_dimensions: int,
    ) -> ActiveIndexSnapshot:
        """Returns an immutable snapshot binding concrete collection + embedding model for one request."""
        with self._meta_lock:
            if self._active_snapshot is not None:
                return self._active_snapshot

        aliases = self.client.get_aliases().aliases
        concrete = next(
            (a.collection_name for a in aliases if a.alias_name == self.active_alias),
            None,
        )
        if concrete is None:
            raise RuntimeError(f"Qdrant alias '{self.active_alias}' is not initialized")

        info = self.client.get_collection(concrete)
        vec_cfg = info.config.params.vectors
        dims = int(vec_cfg.size) if hasattr(vec_cfg, "size") else default_dimensions  # type: ignore[union-attr]

        # Inspect one sample point payload to recover exact model & version after process restart
        points, _ = self.client.scroll(
            collection_name=concrete,
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
        if points and points[0].payload:
            model = str(points[0].payload.get("embedding_model") or default_model)
            version = str(points[0].payload.get("embedding_version") or default_version)
        else:
            with self._meta_lock:
                meta = self._collection_meta.get(concrete)
            model, version = (meta[0], meta[1]) if meta else (default_model, default_version)

        snap = ActiveIndexSnapshot(
            alias_name=self.active_alias,
            collection_name=concrete,
            embedding_model=model,
            embedding_version=version,
            dimensions=dims,
        )
        with self._meta_lock:
            self._active_snapshot = snap
        return snap

    def upsert_document_chunks(
        self,
        *,
        collection_name: str,
        doc_id: str,
        chunks: list[DocumentChunk],
        vectors: list[list[float]],
        source_ts_ms: int,
    ) -> UpsertStats:
        """Idempotently upserts deterministic UUIDv5 chunk points and prunes removed chunks."""
        if len(chunks) != len(vectors):
            raise ValueError("chunks and vectors length mismatch")
        if not chunks:
            deleted = self.delete_document(collection_name=collection_name, doc_id=doc_id)
            return UpsertStats(upserted_count=0, stale_removed_count=deleted, point_ids=[])

        indexed_ts_ms = int(time.time() * 1000)
        points = [
            qmodels.PointStruct(
                id=chunk.point_id,
                vector=vec,
                payload={
                    "doc_id": chunk.doc_id,
                    "chunk_id": f"{chunk.doc_id}#{chunk.chunk_index}",
                    "chunk_index": chunk.chunk_index,
                    "chunk_hash": chunk.chunk_hash,
                    "title": chunk.title,
                    "section": chunk.section,
                    "text": chunk.text,
                    "embedding_model": chunk.embedding_model,
                    "embedding_version": chunk.embedding_version,
                    "source_ts_ms": source_ts_ms,
                    "indexed_ts_ms": indexed_ts_ms,
                },
            )
            for chunk, vec in zip(chunks, vectors, strict=True)
        ]
        self.client.upsert(collection_name=collection_name, points=points, wait=True)

        new_ids = {chunk.point_id for chunk in chunks}
        existing_points, _ = self.client.scroll(
            collection_name=collection_name,
            scroll_filter=qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="doc_id", match=qmodels.MatchValue(value=doc_id)
                    )
                ]
            ),
            limit=500,
            with_payload=False,
            with_vectors=False,
        )
        stale_ids = [str(pt.id) for pt in existing_points if str(pt.id) not in new_ids]
        if stale_ids:
            self.client.delete(
                collection_name=collection_name,
                points_selector=qmodels.PointIdsList(points=stale_ids),
                wait=True,
            )

        CHUNKS_UPSERTED_TOTAL.labels(
            collection=collection_name,
            embedding_model=chunks[0].embedding_model,
            embedding_version=chunks[0].embedding_version,
        ).inc(len(points))

        return UpsertStats(
            upserted_count=len(points),
            stale_removed_count=len(stale_ids),
            point_ids=[c.point_id for c in chunks],
        )

    def delete_document(self, *, collection_name: str, doc_id: str) -> int:
        """Removes all vector chunks for `doc_id` upon receiving a Kafka tombstone."""
        doc_filter = qmodels.Filter(
            must=[
                qmodels.FieldCondition(
                    key="doc_id", match=qmodels.MatchValue(value=doc_id)
                )
            ]
        )
        existing, _ = self.client.scroll(
            collection_name=collection_name,
            scroll_filter=doc_filter,
            limit=500,
            with_payload=False,
            with_vectors=False,
        )
        if not existing:
            return 0
        self.client.delete(
            collection_name=collection_name,
            points_selector=qmodels.FilterSelector(filter=doc_filter),
            wait=True,
        )
        DOCS_DELETED_TOTAL.labels(collection=collection_name).inc()
        return len(existing)

    def search(
        self,
        *,
        collection_name: str,
        query_vector: list[float],
        top_k: int = 4,
    ) -> list[Citation]:
        if hasattr(self.client, "query_points"):
            res = self.client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=top_k,
                with_payload=True,
            )
            scored_points = res.points
        else:
            scored_points = self.client.search(  # type: ignore[attr-defined]
                collection_name=collection_name,
                query_vector=query_vector,
                limit=top_k,
                with_payload=True,
            )

        citations: list[Citation] = []
        for hit in scored_points:
            payload = hit.payload or {}
            citations.append(
                Citation(
                    doc_id=str(payload.get("doc_id", "")),
                    chunk_id=str(payload.get("chunk_id", "")),
                    title=str(payload.get("title", "")),
                    snippet=str(payload.get("text", ""))[:360],
                    score=round(float(hit.score), 4),
                    embedding_model=str(payload.get("embedding_model", "")),
                    embedding_version=str(payload.get("embedding_version", "")),
                )
            )
        return citations

    def count_points(self, collection_name: str, doc_id: str | None = None) -> int:
        flt = None
        if doc_id is not None:
            flt = qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="doc_id", match=qmodels.MatchValue(value=doc_id)
                    )
                ]
            )
        return int(self.client.count(collection_name=collection_name, count_filter=flt, exact=True).count)

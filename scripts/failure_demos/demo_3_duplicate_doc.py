"""Failure Demo 3 (`make demo-duplicate-doc`): Deliver the same document event twice — Qdrant vector store gains zero duplicates.

EXPECTED OUTPUT:
  [STEP 1] Publishing KB-DUP-777 (delivery #1) and waiting for vector indexing...
           Indexed chunks for KB-DUP-777 after delivery #1: 2 (point IDs deterministic UUIDv5)
  [STEP 2] Publishing identical KB-DUP-777 event a second time (replay / duplicate delivery #2)...
           Indexed chunks for KB-DUP-777 after delivery #2: 2 (delta = 0 duplicates!)
  [STEP 3] Publishing delete tombstone for KB-DUP-777...
           Indexed chunks for KB-DUP-777 after tombstone: 0
  [ASSERT PASS] Idempotent UUIDv5 (doc_id + chunk_hash + version) prevented vector duplicates and tombstone purged all chunks.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from realtime_ai.pipeline.vector_store import QdrantVectorRepository


def _execute_demo(
    client: Any,
    vector_repo: QdrantVectorRepository,
    drain_rag_fn: Any = None,
) -> None:
    doc_id = "KB-DUP-777"
    payload = {
        "doc_id": doc_id,
        "op": "create",
        "title": "KB-DUP-777: Idempotent Vector Upsert Verification Runbook",
        "content": (
            "# KB-DUP-777: Idempotent Vector Upsert Verification\n\n"
            "## Deterministic Point Identity\n"
            "Every chunk point ID in Qdrant is derived via UUIDv5 over "
            "(doc_id, chunk_index, sha256(sanitized_chunk_text), embedding_version). "
            "Replaying Kafka offsets or delivering the exact same CDC change event multiple "
            "times overwrites identical point IDs in place without creating duplicate vectors.\n\n"
            "## Tombstone Cleanup\n"
            "Deleting a document emits a null-payload Kafka tombstone keyed by doc_id, "
            "which deletes every vector point matching doc_id from Qdrant."
        ),
        "source_uri": "demo://duplicate-delivery",
    }

    health = client.get("/healthz").json()
    collection = health["active_index"]["collection_name"]
    baseline_total = vector_repo.count_points(collection)

    print(f"[STEP 1] Publishing {doc_id} (delivery #1) to {collection}...")
    r1 = client.post("/v1/kb/documents", json=payload)
    assert r1.status_code == 202, r1.text

    deadline = time.monotonic() + 25.0
    count_after_1 = 0
    while time.monotonic() < deadline:
        if drain_rag_fn is not None:
            drain_rag_fn()
        count_after_1 = vector_repo.count_points(collection, doc_id=doc_id)
        if count_after_1 > 0:
            break
        time.sleep(0.2)
    assert count_after_1 > 0, f"{doc_id} was not indexed within timeout"
    total_after_1 = vector_repo.count_points(collection)
    print(
        f"         Indexed chunks for {doc_id} after delivery #1: {count_after_1} "
        f"(collection total: {baseline_total} -> {total_after_1})"
    )

    prev_samples = int(client.get("/v1/freshness").json()["freshness"]["samples_observed"] or 0)
    print(f"[STEP 2] Publishing identical {doc_id} event a second time (duplicate delivery #2)...")
    r2 = client.post("/v1/kb/documents", json=payload)
    assert r2.status_code == 202, r2.text

    deadline = time.monotonic() + 25.0
    while time.monotonic() < deadline:
        if drain_rag_fn is not None:
            drain_rag_fn()
        cur_samples = int(client.get("/v1/freshness").json()["freshness"]["samples_observed"] or 0)
        if cur_samples > prev_samples:
            break
        time.sleep(0.2)

    count_after_2 = vector_repo.count_points(collection, doc_id=doc_id)
    total_after_2 = vector_repo.count_points(collection)
    print(
        f"         Indexed chunks for {doc_id} after delivery #2: {count_after_2} "
        f"(collection total: {total_after_2}, delta = {count_after_2 - count_after_1} duplicates!)"
    )
    assert count_after_2 == count_after_1, (
        f"Duplicate vectors created! Expected {count_after_1}, got {count_after_2}"
    )
    assert total_after_2 == total_after_1, (
        f"Collection gained unexpected points: {total_after_1} -> {total_after_2}"
    )

    print(f"[STEP 3] Publishing delete tombstone for {doc_id}...")
    r_del = client.delete(f"/v1/kb/documents/{doc_id}")
    assert r_del.status_code == 202

    deadline = time.monotonic() + 25.0
    count_after_del = count_after_2
    while time.monotonic() < deadline:
        if drain_rag_fn is not None:
            drain_rag_fn()
        count_after_del = vector_repo.count_points(collection, doc_id=doc_id)
        if count_after_del == 0:
            break
        time.sleep(0.2)

    print(f"         Indexed chunks for {doc_id} after tombstone: {count_after_del}")
    assert count_after_del == 0, f"Tombstone failed to purge vectors (remaining={count_after_del})"
    print(
        "[ASSERT PASS] Idempotent UUIDv5 (doc_id + chunk_hash + version) prevented vector "
        "duplicates and tombstone purged all chunks."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()

    if args.in_memory:
        from scripts._harness import InMemoryPipelineHarness

        harness = InMemoryPipelineHarness()
        try:
            harness.seed_and_index_kb()
            _execute_demo(harness.client, harness.vector_repo, drain_rag_fn=harness.drain_rag)
        finally:
            harness.close()
        return

    vec_repo = QdrantVectorRepository(url=args.qdrant_url)
    with httpx.Client(base_url=args.api_url, timeout=25.0) as client:
        _execute_demo(client, vec_repo)


if __name__ == "__main__":
    main()

"""Integration tests for Part A: streaming retrieval, PII redaction, tombstones, freshness, and zero-downtime replay switchover."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from realtime_ai.kafka.topics import TOPIC_KB_SANITIZED
from scripts._harness import InMemoryPipelineHarness
from scripts.evaluate_retrieval import evaluate_before_and_after_switch


@pytest.mark.integration
def test_part_a_watcher_pii_redaction_idempotent_upsert_tombstone_and_freshness(
    pipeline_harness: InMemoryPipelineHarness,
) -> None:
    seeded = pipeline_harness.seed_and_index_kb()
    assert seeded >= 8

    # Live file creation via watched directory with embedded PII & secret
    live_file = pipeline_harness.watch_dir / "KB-900-live-hotfix.md"
    live_file.write_text(
        "# KB-900: Live Hotfix Window\n"
        "On-call contact pager@acme.internal (+1-415-555-0133) deployed patch 4.4.0-hotfix2 "
        "using temporary key sk-live-HotfixSecretKey1234567890. Zero broker restarts required.\n",
        encoding="utf-8",
    )
    emitted = pipeline_harness.watcher.scan_once()
    assert emitted == 1
    pipeline_harness.drain_rag()

    # Verify PII never leaked into sanitized Kafka topic or Qdrant vector payload
    sanitized_events = [
        pipeline_harness.codec.decode(TOPIC_KB_SANITIZED, r.value)
        for r in pipeline_harness.cluster.all_records(TOPIC_KB_SANITIZED)
        if r.key == b"KB-900" and r.value is not None
    ]
    assert len(sanitized_events) == 1
    assert sanitized_events[0] is not None
    assert "pager@acme.internal" not in sanitized_events[0]["sanitized_content"]
    assert "sk-live-" not in sanitized_events[0]["sanitized_content"]
    assert sanitized_events[0]["pii_redactions_count"] >= 3

    # Query /v1/qa -> retrieves KB-900 with citation + measured freshness
    qa_resp = pipeline_harness.client.post(
        "/v1/qa",
        json={"question": "What hotfix patch version was deployed with zero broker restarts?", "top_k": 3},
    )
    assert qa_resp.status_code == 200
    qa_body = qa_resp.json()
    assert any(c["doc_id"] == "KB-900" for c in qa_body["citations"])
    assert "pager@acme.internal" not in qa_body["answer"]
    assert qa_body["freshness"]["samples_observed"] >= 9
    assert qa_body["freshness"]["last_freshness_ms"] >= 0.0
    assert qa_body["freshness"]["p95_freshness_ms"] < 5000.0

    # Deliver exact same document event a second time -> 0 duplicate vectors
    active_col = qa_body["active_index"]["collection_name"]
    before_count = pipeline_harness.vector_repo.count_points(active_col, doc_id="KB-900")
    assert before_count >= 1
    dup_resp = pipeline_harness.client.post(
        "/v1/kb/documents",
        json={
            "doc_id": "KB-900",
            "event_id": "evt-dup-900",
            "title": "KB-900: Live Hotfix Window",
            "content": live_file.read_text(encoding="utf-8"),
        },
    )
    assert dup_resp.status_code == 202
    pipeline_harness.drain_rag()
    after_dup_count = pipeline_harness.vector_repo.count_points(active_col, doc_id="KB-900")
    assert after_dup_count == before_count

    # Delete file -> watcher emits Kafka tombstone -> vectors removed from Qdrant
    live_file.unlink()
    assert pipeline_harness.watcher.scan_once() == 1
    pipeline_harness.drain_rag()
    assert pipeline_harness.vector_repo.count_points(active_col, doc_id="KB-900") == 0


@pytest.mark.integration
def test_part_a_zero_downtime_replay_reembed_improves_retrieval_benchmark(
    pipeline_harness: InMemoryPipelineHarness,
) -> None:
    pipeline_harness.seed_and_index_kb()
    eval_path = Path(__file__).resolve().parents[2] / "data" / "eval" / "retrieval_eval.json"
    queries = json.loads(eval_path.read_text(encoding="utf-8"))["queries"]

    report = evaluate_before_and_after_switch(
        pipeline_harness.client,
        queries,
        new_model="local-semantic-v2",
        new_version="v2",
    )
    assert report["before_switch"]["active_index"]["embedding_version"] == "v1"
    assert report["before_switch"]["active_index"]["dimensions"] == 384
    assert report["after_switch"]["active_index"]["embedding_version"] == "v2"
    assert report["after_switch"]["active_index"]["dimensions"] == 768
    assert report["replay_cutover"]["records_replayed"] >= 8
    assert report["after_switch"]["recall_at_1"] > report["before_switch"]["recall_at_1"]
    assert report["after_switch"]["recall_at_1"] >= 0.95
    assert report["after_switch"]["recall_at_3"] == 1.0

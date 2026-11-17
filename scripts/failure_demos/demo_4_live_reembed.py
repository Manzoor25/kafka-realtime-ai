"""Failure Demo 4 (`make demo-live-reembed`): Switch embedding models by Kafka replay while questions keep being answered with zero downtime.

EXPECTED OUTPUT:
  [STEP 1] Starting continuous /v1/qa reader traffic before, during, and after replay cutover...
  [STEP 2] Triggering compacted Kafka replay into second Qdrant index (v2, dim=768) + atomic alias swap...
           Replay completed: records_replayed=8 points=N replay_ms=... cutover_ms=...
  [STEP 3] Draining post-cutover QA requests and auditing availability...
  [ASSERT PASS] Answered N continuous QA queries across v1 -> v2 model switch with 0 errors (100.0% uptime).
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

_QUESTIONS = [
    "What is the maximum SAML clock skew tolerance in Settings?",
    "How does dual-secret rotation format X-Webhook-Signature?",
    "How fast does Emergency Revoke propagate for a leaked API key?",
    "What is the GDPR Article 17 right to erasure fulfillment SLA?",
]


def _run_live_switch(
    make_qa_call: Any,
    trigger_reembed_call: Any,
    target_model: str,
    target_version: str,
) -> None:
    stop_traffic = threading.Event()
    versions_seen: list[str] = []
    latencies_ms: list[float] = []
    errors: list[str] = []
    lock = threading.Lock()

    def _qa_reader_loop() -> None:
        idx = 0
        while not stop_traffic.is_set():
            q = _QUESTIONS[idx % len(_QUESTIONS)]
            idx += 1
            try:
                status_code, body = make_qa_call(q)
                if status_code != 200:
                    with lock:
                        errors.append(f"HTTP {status_code}: {body}")
                else:
                    ver = body["active_index"]["embedding_version"]
                    with lock:
                        versions_seen.append(ver)
                        latencies_ms.append(float(body["latency_ms"]))
            except Exception as exc:
                with lock:
                    errors.append(str(exc))
            time.sleep(0.04)

    print("[STEP 1] Starting continuous /v1/qa reader traffic before, during, and after replay cutover...")
    reader = threading.Thread(target=_qa_reader_loop, daemon=True)
    reader.start()

    # Let pre-switch traffic accumulate on the initial version
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        with lock:
            if len(versions_seen) >= 5:
                break
        time.sleep(0.05)

    print(
        f"[STEP 2] Triggering compacted Kafka replay into second Qdrant index "
        f"({target_model} / {target_version}) + atomic alias swap..."
    )
    cutover_report = trigger_reembed_call(target_model, target_version)
    print(
        f"         Replay completed: collection={cutover_report['target_collection']} "
        f"dim={cutover_report['dimensions']} records_replayed={cutover_report['records_replayed']} "
        f"points={cutover_report['points_in_new_index']} "
        f"replay_ms={cutover_report['replay_duration_ms']} "
        f"cutover_ms={cutover_report['cutover_duration_ms']}"
    )

    print("[STEP 3] Running post-cutover QA traffic on new index and auditing zero-downtime availability...")
    post_deadline = time.monotonic() + 10.0
    while time.monotonic() < post_deadline:
        with lock:
            if versions_seen.count(target_version) >= 5:
                break
        time.sleep(0.05)

    stop_traffic.set()
    reader.join(timeout=5.0)

    distinct_versions = list(dict.fromkeys(versions_seen))
    assert not errors, f"Zero-downtime violation! QA errors during model switch: {errors[:3]}"
    assert len(distinct_versions) >= 2 and target_version in distinct_versions, (
        f"Expected live transition into {target_version}; observed versions={distinct_versions}"
    )
    avg_ms = round(sum(latencies_ms) / max(1, len(latencies_ms)), 2)
    print(
        f"[ASSERT PASS] Answered {len(versions_seen)} continuous QA queries across "
        f"{' -> '.join(distinct_versions)} model switch with 0 errors (100.0% uptime, mean_qa_ms={avg_ms})."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--new-model", default=None)
    parser.add_argument("--new-version", default="v2-live")
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()

    if args.in_memory:
        from scripts._harness import InMemoryPipelineHarness

        harness = InMemoryPipelineHarness()
        try:
            harness.seed_and_index_kb()
            client_lock = threading.Lock()

            def _qa_mem(q: str) -> tuple[int, dict[str, Any]]:
                with client_lock:
                    r = harness.client.post(
                        "/v1/qa", json={"question": q, "top_k": 2, "retrieval_only": True}
                    )
                    return r.status_code, r.json()

            def _reembed_mem(model: str, ver: str) -> dict[str, Any]:
                # Run replay directly via pipeline function so reader thread keeps querying concurrently
                from realtime_ai.pipeline.reembed import replay_and_switch_index

                rep = replay_and_switch_index(
                    settings=harness.settings,
                    codec=harness.codec,
                    producer=harness.producer,
                    provider=harness.provider,
                    vector_repo=harness.vector_repo,
                    new_embedding_model=model,
                    new_embedding_version=ver,
                )
                return rep.__dict__

            _run_live_switch(
                _qa_mem,
                _reembed_mem,
                target_model=args.new_model or "local-semantic-v2",
                target_version=args.new_version,
            )
        finally:
            harness.close()
        return

    target_model = args.new_model or "nomic-embed-text:v1.5"

    def _qa_http(q: str) -> tuple[int, dict[str, Any]]:
        with httpx.Client(base_url=args.api_url, timeout=30.0) as c:
            r = c.post("/v1/qa", json={"question": q, "top_k": 2})
            return r.status_code, r.json()

    def _reembed_http(model: str, ver: str) -> dict[str, Any]:
        with httpx.Client(base_url=args.api_url, timeout=180.0) as c:
            r = c.post(
                "/v1/kb/reembed",
                json={"new_embedding_model": model, "new_embedding_version": ver},
            )
            r.raise_for_status()
            return r.json()

    _run_live_switch(_qa_http, _reembed_http, target_model, args.new_version)


if __name__ == "__main__":
    main()

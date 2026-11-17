"""Retrieval benchmark (`make eval`): measures Recall@1, Recall@3, and MRR@3 before and after Kafka replay model switchover."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _score_queries(client: Any, queries: list[dict[str, Any]]) -> dict[str, Any]:
    hits_at_1 = 0
    hits_at_3 = 0
    rr_sum = 0.0
    by_cat: dict[str, list[float]] = {"lexical": [], "semantic_paraphrase": []}
    per_query: list[dict[str, Any]] = []
    active_index: dict[str, Any] = {}

    for q in queries:
        resp = client.post(
            "/v1/qa",
            json={"question": q["question"], "top_k": 3, "retrieval_only": True},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        active_index = body["active_index"]
        ranked_docs: list[str] = []
        for c in body["citations"]:
            if c["doc_id"] not in ranked_docs:
                ranked_docs.append(c["doc_id"])

        expected = q["expected_doc_id"]
        hit1 = bool(ranked_docs and ranked_docs[0] == expected)
        hit3 = expected in ranked_docs[:3]
        rr = (1.0 / (ranked_docs.index(expected) + 1)) if hit3 else 0.0

        hits_at_1 += int(hit1)
        hits_at_3 += int(hit3)
        rr_sum += rr
        by_cat.setdefault(q["category"], []).append(1.0 if hit1 else 0.0)
        per_query.append(
            {
                "id": q["id"],
                "category": q["category"],
                "expected": expected,
                "top3": ranked_docs[:3],
                "hit_at_1": hit1,
                "hit_at_3": hit3,
                "rr": round(rr, 4),
            }
        )

    n = len(queries)
    lex_scores = by_cat.get("lexical", [])
    sem_scores = by_cat.get("semantic_paraphrase", [])
    return {
        "active_index": active_index,
        "query_count": n,
        "recall_at_1": round(hits_at_1 / n, 4),
        "recall_at_3": round(hits_at_3 / n, 4),
        "mrr_at_3": round(rr_sum / n, 4),
        "lexical_recall_at_1": round(sum(lex_scores) / max(1, len(lex_scores)), 4),
        "semantic_paraphrase_recall_at_1": round(sum(sem_scores) / max(1, len(sem_scores)), 4),
        "details": per_query,
    }


def evaluate_before_and_after_switch(
    client: Any,
    queries: list[dict[str, Any]],
    new_model: str,
    new_version: str = "v2",
) -> dict[str, Any]:
    before = _score_queries(client, queries)
    switch_resp = client.post(
        "/v1/kb/reembed",
        json={
            "new_embedding_model": new_model,
            "new_embedding_version": new_version,
            "reason": "retrieval_eval_benchmark",
        },
    )
    assert switch_resp.status_code == 200, switch_resp.text
    cutover = switch_resp.json()
    after = _score_queries(client, queries)
    return {
        "before_switch": before,
        "replay_cutover": cutover,
        "after_switch": after,
        "delta_recall_at_1": round(after["recall_at_1"] - before["recall_at_1"], 4),
        "delta_recall_at_3": round(after["recall_at_3"] - before["recall_at_3"], 4),
        "delta_mrr_at_3": round(after["mrr_at_3"] - before["mrr_at_3"], 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--provider", choices=["stack", "local"], default="stack")
    parser.add_argument("--new-model", default=None)
    parser.add_argument("--new-version", default="v2")
    parser.add_argument("--output-json", default=None)
    args = parser.parse_args()

    eval_path = Path(__file__).resolve().parents[1] / "data" / "eval" / "retrieval_eval.json"
    dataset = json.loads(eval_path.read_text(encoding="utf-8"))
    queries = dataset["queries"]

    if args.provider == "local":
        from scripts._harness import InMemoryPipelineHarness

        harness = InMemoryPipelineHarness()
        try:
            harness.seed_and_index_kb()
            target_model = args.new_model or "local-semantic-v2"
            report = evaluate_before_and_after_switch(
                harness.client, queries, new_model=target_model, new_version=args.new_version
            )
        finally:
            harness.close()
    else:
        target_model = args.new_model or "nomic-embed-text:v1.5"
        with httpx.Client(base_url=args.api_url, timeout=120.0) as client:
            report = evaluate_before_and_after_switch(
                client, queries, new_model=target_model, new_version=args.new_version
            )

    b = report["before_switch"]
    a = report["after_switch"]
    c = report["replay_cutover"]
    print("=" * 78)
    print(" RETRIEVAL EVALUATION BEFORE & AFTER ZERO-DOWNTIME KAFKA REPLAY SWITCHOVER")
    print("=" * 78)
    print(
        f"  BEFORE ({b['active_index']['embedding_model']} / {b['active_index']['embedding_version']}, "
        f"dim={b['active_index']['dimensions']}): "
        f"Recall@1={b['recall_at_1']:.2%} | Recall@3={b['recall_at_3']:.2%} | MRR@3={b['mrr_at_3']:.4f} "
        f"(lexical={b['lexical_recall_at_1']:.2%}, paraphrase={b['semantic_paraphrase_recall_at_1']:.2%})"
    )
    print(
        f"  REPLAY & CUTOVER -> {c['target_collection']}: "
        f"records_replayed={c['records_replayed']} points={c['points_in_new_index']} "
        f"replay={c['replay_duration_ms']}ms atomic_cutover={c['cutover_duration_ms']}ms"
    )
    print(
        f"  AFTER  ({a['active_index']['embedding_model']} / {a['active_index']['embedding_version']}, "
        f"dim={a['active_index']['dimensions']}): "
        f"Recall@1={a['recall_at_1']:.2%} | Recall@3={a['recall_at_3']:.2%} | MRR@3={a['mrr_at_3']:.4f} "
        f"(lexical={a['lexical_recall_at_1']:.2%}, paraphrase={a['semantic_paraphrase_recall_at_1']:.2%})"
    )
    print(
        f"  DELTA: Recall@1={report['delta_recall_at_1']:+.2%} | "
        f"Recall@3={report['delta_recall_at_3']:+.2%} | MRR@3={report['delta_mrr_at_3']:+.4f}"
    )

    if args.output_json:
        Path(args.output_json).write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

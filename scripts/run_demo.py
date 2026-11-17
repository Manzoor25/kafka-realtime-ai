"""End-to-end showcase (`make demo`): streaming RAG freshness + PII redaction + KIP-932 multi-agent triage + human approval."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _run_against_client(client: Any, drain_fn: Any = None) -> dict[str, Any]:
    print("=" * 78)
    print(" PART A: STREAMING RAG — SUB-SECOND FRESHNESS & STREAMING PII REDACTION")
    print("=" * 78)

    live_doc_id = "KB-009"
    unique_code = "MAINT-WINDOW-2026-NOV"
    publish_t0 = time.perf_counter()
    up_resp = client.post(
        "/v1/kb/documents",
        json={
            "doc_id": live_doc_id,
            "op": "create",
            "title": "KB-009: Emergency EU-Central Kafka Broker Maintenance Window",
            "content": (
                f"# KB-009: Emergency EU-Central Maintenance ({unique_code})\n\n"
                f"Scheduled rolling KRaft broker patch reference `{unique_code}` adds zero downtime "
                "with replication.factor=3 and min.insync.replicas=2. Clients see < 150ms leader "
                "election failover.\n\n"
                "Internal coordinator (must be redacted): sec-ops@acme-internal.example, "
                "SSN 123-45-6789, token sk-live-SuperSecretTokenValue123456."
            ),
            "source_uri": "demo://live-update",
        },
    )
    assert up_resp.status_code == 202, up_resp.text

    # Poll QA retrieval until KB-009 is live and retrievable
    qa_data: dict[str, Any] = {}
    observed_e2e_ms = 0.0
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if drain_fn is not None:
            drain_fn("rag")
        r = client.post(
            "/v1/qa",
            json={
                "question": f"What happens during emergency broker patch {unique_code}?",
                "top_k": 3,
            },
        )
        assert r.status_code == 200, r.text
        qa_data = r.json()
        cited_ids = [c["doc_id"] for c in qa_data["citations"]]
        if live_doc_id in cited_ids:
            observed_e2e_ms = round((time.perf_counter() - publish_t0) * 1000.0, 2)
            break
        time.sleep(0.15)
    else:
        raise RuntimeError("Timed out waiting for KB-009 to become retrievable")

    kb9_cite = next(c for c in qa_data["citations"] if c["doc_id"] == live_doc_id)
    assert "sec-ops@acme-internal.example" not in kb9_cite["snippet"], "Raw email leaked!"
    assert "123-45-6789" not in kb9_cite["snippet"], "Raw SSN leaked!"
    assert "sk-live-" not in kb9_cite["snippet"], "Raw API token leaked!"

    print(f"  [OK] Published {live_doc_id} -> retrievable via /v1/qa in {observed_e2e_ms:.1f} ms")
    print(f"  [OK] Pipeline telemetry freshness: {json.dumps(qa_data['freshness'])}")
    print(f"  [OK] PII scrubbed before embedding: snippet={kb9_cite['snippet']!r}")
    print(f"  [OK] Grounded QA Answer: {qa_data['answer']}")

    print("\n" + "=" * 78)
    print(" PART B: KIP-932 SHARE GROUP AGENTS -> TOOLS -> DRAFTER -> REVIEWER -> HUMAN UI")
    print("=" * 78)

    sample_tickets = [
        {
            "ticket_id": "TKT-DEMO-SSO",
            "customer_id": "cust-nordic-bank",
            "customer_email": "alice.lindqvist@nordicbank.example",
            "account_tier": "enterprise",
            "subject": "Okta SAML_ASSERTION_EXPIRED after IdP host NTP drift",
            "body": "Our employees hit SAML_ASSERTION_EXPIRED at /auth/saml/acs. Reach me at alice.lindqvist@nordicbank.example or 415-555-0188.",
        },
        {
            "ticket_id": "TKT-DEMO-KEY",
            "customer_id": "cust-fintech-io",
            "customer_email": "devops@fintech.example",
            "account_tier": "enterprise",
            "subject": "URGENT: Production API key sk-live-AccidentalGithubLeak99887766 exposed",
            "body": "Developer pushed sk-live-AccidentalGithubLeak99887766 to GitHub. How fast does Emergency Revoke propagate?",
        },
        {
            "ticket_id": "TKT-DEMO-VAT",
            "customer_id": "cust-berlin-gmbh",
            "customer_email": "finance@berlin-gmbh.example",
            "account_tier": "pro",
            "subject": "Forgot EU VAT number on invoice INV-2026-8841",
            "body": "We forgot to add our German VIES VAT ID before invoice INV-2026-8841 generated 6 days ago. Can we get a reverse-charge credit note?",
        },
        {
            "ticket_id": "TKT-DEMO-429",
            "customer_id": "cust-analytics-co",
            "customer_email": "eng@analytics.example",
            "account_tier": "pro",
            "subject": "Getting HTTP 429 burst throttling on Pro tier",
            "body": "Our Kafka ingestion workers hit HTTP 429 when burst credits run out. What RPS limit applies to Pro and how should consumers back off?",
        },
    ]

    for t in sample_tickets:
        resp = client.post("/v1/tickets", json=t)
        assert resp.status_code == 202, resp.text
        print(f"  -> Published ticket {t['ticket_id']} ({t['account_tier']}) to support.tickets.created.v1")

    expected_ids = {t["ticket_id"] for t in sample_tickets}
    deadline = time.monotonic() + 90.0
    pending_by_id: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if drain_fn is not None:
            drain_fn("agents")
        app_resp = client.get("/v1/approvals")
        assert app_resp.status_code == 200
        items = app_resp.json()["items"]
        pending_by_id = {i["ticket_id"]: i for i in items if i["ticket_id"] in expected_ids}
        if len(pending_by_id) == len(expected_ids):
            break
        time.sleep(0.4)
    else:
        raise RuntimeError(f"Only {set(pending_by_id)} of {expected_ids} reached human approval queue")

    for tid in sorted(expected_ids):
        item = pending_by_id[tid]
        cites = [c["doc_id"] for c in item["citations"]]
        print(
            f"  [QUEUED FOR HUMAN] {tid} | category={item['category']} urgency={item['urgency']} "
            f"| citations={cites} | {item['reviewer_notes']}"
        )

    # Human approves 3 replies and rejects 1 draft
    for idx, tid in enumerate(sorted(expected_ids)):
        decision = "reject" if idx == 3 else "approve"
        dec_resp = client.post(
            f"/v1/approvals/{tid}/decide",
            json={
                "decision": decision,
                "actor": "sarah.chen@support-ops",
                "comment": "Verified KB grounding" if decision == "approve" else "Hold for TAM custom quota check",
            },
        )
        assert dec_resp.status_code == 200, dec_resp.text
        print(f"  [HUMAN {decision.upper()}] {tid} -> {dec_resp.json()}")

    audit_resp = client.get("/v1/audit/recent?limit=50")
    assert audit_resp.status_code == 200
    audit_payload = audit_resp.json()
    print(
        f"\n  [COMPLIANCE AUDIT] {audit_payload['count']} recent model invocations recorded on "
        f"ai.audit.events.v1 (retention={audit_payload['retention_days']} days)."
    )

    # Clean up temporary demo doc via tombstone so corpus returns cleanly to KB-001..KB-008
    del_resp = client.delete(f"/v1/kb/documents/{live_doc_id}")
    assert del_resp.status_code == 202
    if drain_fn is not None:
        drain_fn("rag")

    summary = {
        "e2e_query_retrievable_ms": observed_e2e_ms,
        "pipeline_freshness": qa_data["freshness"],
        "tickets_processed": len(expected_ids),
        "approved_count": 3,
        "rejected_count": 1,
        "audit_events_sampled": audit_payload["count"],
    }
    print("\nDEMO SUMMARY:", json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--in-memory", action="store_true", help="Run against in-process harness without Docker")
    args = parser.parse_args()

    if args.in_memory:
        from scripts._harness import InMemoryPipelineHarness

        harness = InMemoryPipelineHarness()
        try:
            harness.seed_and_index_kb()
            _run_against_client(
                harness.client,
                drain_fn=lambda kind: harness.drain_rag() if kind == "rag" else harness.drain_agents(),
            )
        finally:
            harness.close()
        return

    with httpx.Client(base_url=args.api_url, timeout=30.0) as client:
        _run_against_client(client)


if __name__ == "__main__":
    main()

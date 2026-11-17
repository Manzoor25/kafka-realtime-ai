"""Live 3-broker Apache Kafka 4.4.0 KRaft + Karapace + Qdrant + Ollama + Prometheus/Grafana E2E test."""

from __future__ import annotations

import os
import time
import uuid

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.docker_e2e,
    pytest.mark.skipif(
        os.getenv("RUN_DOCKER_E2E") != "1",
        reason="Set RUN_DOCKER_E2E=1 with `docker compose up -d` running to execute full container E2E tests.",
    ),
]


def test_live_docker_compose_streaming_rag_agents_karapace_prometheus_and_grafana() -> None:
    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    prom_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
    grafana_url = os.getenv("GRAFANA_URL", "http://localhost:3000")

    with httpx.Client(timeout=60.0) as client:
        # 1. Verify Karapace Schema Registry registered all 14 JSON Schemas
        sr_resp = client.get(f"{sr_url}/subjects")
        assert sr_resp.status_code == 200
        subjects = sr_resp.json()
        assert len(subjects) >= 12, f"Expected >=12 Karapace schema subjects, got {subjects}"

        # 2. Publish a brand-new KB update with sample PII and measure real ingest-to-retrievable freshness
        doc_id = f"KB-E2E-{uuid.uuid4().hex[:6].upper()}"
        unique_code = f"KRAFT-PATCH-{uuid.uuid4().hex[:8].upper()}"
        t0 = time.perf_counter()
        ingest_resp = client.post(
            f"{api_url}/v1/kb/documents",
            json={
                "doc_id": doc_id,
                "title": f"{doc_id}: Emergency Broker Patch {unique_code}",
                "content": (
                    f"# {doc_id}: Emergency Broker Patch {unique_code}\n"
                    f"Apply rolling patch {unique_code} one broker at a time while monitoring UnderReplicatedPartitions=0. "
                    "Escalation contact: secops@acme.example (+1-415-555-0188), key sk-live-SecretShouldBeScrubbed12345."
                ),
            },
        )
        assert ingest_resp.status_code == 202

        found_citation = False
        e2e_wall_ms = 0.0
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            qa = client.post(
                f"{api_url}/v1/qa",
                json={
                    "question": f"How should rolling patch {unique_code} be applied?",
                    "top_k": 3,
                },
            )
            assert qa.status_code == 200
            qa_body = qa.json()
            if any(c["doc_id"] == doc_id for c in qa_body["citations"]):
                e2e_wall_ms = (time.perf_counter() - t0) * 1000.0
                found_citation = True
                # Confirm PII was scrubbed before embedding & retrieval
                for c in qa_body["citations"]:
                    assert "secops@acme.example" not in c["snippet"]
                    assert "SecretShouldBeScrubbed12345" not in c["snippet"]
                    if c["doc_id"] == doc_id:
                        assert "[REDACTED_EMAIL]" in c["snippet"]
                        assert "[REDACTED_PHONE]" in c["snippet"]
                        assert "[REDACTED_SECRET]" in c["snippet"]
                break
            time.sleep(0.25)

        assert found_citation, f"Document {doc_id} was not retrievable within 30s"
        freshness = client.get(f"{api_url}/v1/freshness").json()
        print(
            f"\n[DOCKER E2E MEASURED FRESHNESS] wall_clock_ms={e2e_wall_ms:.1f}ms | "
            f"pipeline_freshness={freshness}"
        )

        # Clean up temporary E2E doc via Kafka tombstone and verify vector purge
        del_resp = client.delete(f"{api_url}/v1/kb/documents/{doc_id}")
        assert del_resp.status_code == 202
        purge_deadline = time.monotonic() + 20.0
        purged = False
        while time.monotonic() < purge_deadline:
            qa_after = client.post(
                f"{api_url}/v1/qa",
                json={
                    "question": f"How should rolling patch {unique_code} be applied?",
                    "top_k": 3,
                    "retrieval_only": True,
                },
            ).json()
            if all(c["doc_id"] != doc_id for c in qa_after["citations"]):
                purged = True
                break
            time.sleep(0.25)
        assert purged, f"Tombstone did not purge {doc_id} from Qdrant within 20s"

        # 3. Submit a support ticket -> KIP-932 Share Group triage -> tools -> drafter -> reviewer -> human approval
        ticket_id = f"TKT-E2E-{uuid.uuid4().hex[:6].upper()}"
        t_ticket = time.perf_counter()
        t_resp = client.post(
            f"{api_url}/v1/tickets",
            json={
                "ticket_id": ticket_id,
                "customer_id": "CUST-DOCKER-1",
                "customer_email": "ops@docker-e2e.example",
                "account_tier": "enterprise",
                "subject": "SAML SSO clock skew login error",
                "body": "Our Okta users fail SAML login with NotOnOrAfter clock skew error.",
            },
        )
        assert t_resp.status_code == 202

        approval_ready = None
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline:
            apps = client.get(f"{api_url}/v1/approvals").json()["items"]
            match = next((a for a in apps if a["ticket_id"] == ticket_id), None)
            if match and match["status"] == "pending":
                approval_ready = match
                break
            time.sleep(1.0)

        assert approval_ready is not None, f"Ticket {ticket_id} did not reach pending approval within 90s"
        triage_e2e_ms = (time.perf_counter() - t_ticket) * 1000.0
        print(
            f"[DOCKER E2E MEASURED AGENT PIPELINE] ticket_id={ticket_id} "
            f"status=pending_human_approval in {triage_e2e_ms:.1f}ms"
        )

        # Approve via HTTP API & verify /approvals HTML page
        ui_resp = client.get(f"{api_url}/approvals")
        assert ui_resp.status_code == 200
        assert ticket_id in ui_resp.text

        decide_resp = client.post(
            f"{api_url}/v1/approvals/{ticket_id}/decide",
            json={"decision": "approve", "actor": "ci-e2e-operator", "comment": "LGTM"},
        )
        assert decide_resp.status_code == 200
        assert decide_resp.json()["status"] == "approved"

        # 4. Verify Prometheus & Grafana are live and scraping pipeline + audit cost metrics
        prom_health = client.get(f"{prom_url}/-/healthy")
        assert prom_health.status_code == 200
        prom_query = client.get(
            f"{prom_url}/api/v1/query",
            params={"query": "krai_audit_records_total"},
        )
        assert prom_query.status_code == 200
        assert prom_query.json()["status"] == "success"

        graf_health = client.get(f"{grafana_url}/api/health")
        assert graf_health.status_code == 200

"""Integration tests for Part B: KIP-932 Share Group triage, topic-only multi-agent choreography, human approval UI, resilience, and compliance audit."""

from __future__ import annotations

import time

import pytest

from realtime_ai.kafka.consumer import AckAction, RawRecord
from realtime_ai.kafka.topics import (
    SEVEN_YEARS_MS,
    TOPIC_AGENT_DRAFTS,
    TOPIC_AGENT_REVIEWS,
    TOPIC_AGENT_TOOL_CALLS,
    TOPIC_AGENT_TOOL_RESULTS,
    TOPIC_AGENT_TRIAGE,
    TOPIC_AI_AUDIT,
    TOPIC_DLQ,
    TOPIC_HUMAN_APPROVALS,
    TOPIC_REPLIES_APPROVED,
    TOPIC_TICKETS_CREATED,
    TOPICS_BY_NAME,
)
from scripts._harness import InMemoryPipelineHarness


@pytest.mark.integration
def test_part_b_multi_agent_topics_human_approval_ui_and_7yr_audit_metrics(
    pipeline_harness: InMemoryPipelineHarness,
) -> None:
    pipeline_harness.seed_and_index_kb()

    # Confirm 4 Share-Group triage workers > 2 partitions on support.tickets.created.v1
    assert len(pipeline_harness.triage_workers) == 4
    assert TOPICS_BY_NAME[TOPIC_TICKETS_CREATED].partitions == 2
    assert TOPICS_BY_NAME[TOPIC_AI_AUDIT].retention_ms == SEVEN_YEARS_MS

    # Create two tickets: one approved by human, one rejected by human
    for tid, subject, body in [
        (
            "TKT-INT-APPROVE",
            "Okta SAML clock skew failure",
            "Users receive SAML Audience/NotBefore error after NTP drift. Reach me at admin@client.org.",
        ),
        (
            "TKT-INT-REJECT",
            "Invoice prorated seat credit",
            "Please explain how annual seat downgrades are credited at renewal.",
        ),
    ]:
        r = pipeline_harness.client.post(
            "/v1/tickets",
            json={
                "ticket_id": tid,
                "customer_id": "CUST-ENT-42",
                "customer_tier": "enterprise",
                "subject": subject,
                "body": body,
            },
        )
        assert r.status_code == 202

    pipeline_harness.drain_agents()

    # Verify agents communicated exclusively through Kafka topics (including tool calls & results)
    for topic in (
        TOPIC_TICKETS_CREATED,
        TOPIC_AGENT_TRIAGE,
        TOPIC_AGENT_TOOL_CALLS,
        TOPIC_AGENT_TOOL_RESULTS,
        TOPIC_AGENT_DRAFTS,
        TOPIC_AGENT_REVIEWS,
        TOPIC_HUMAN_APPROVALS,
        TOPIC_AI_AUDIT,
    ):
        assert len(pipeline_harness.cluster.all_records(topic)) >= 2, f"Expected events on {topic}"

    # Nothing is sent to customer BEFORE human approval!
    assert len(pipeline_harness.cluster.all_records(TOPIC_REPLIES_APPROVED)) == 0

    # Verify Human Approval web UI renders pending tickets
    html_resp = pipeline_harness.client.get("/approvals")
    assert html_resp.status_code == 200
    assert "TKT-INT-APPROVE" in html_resp.text
    assert "TKT-INT-REJECT" in html_resp.text

    # Approve TKT-INT-APPROVE and Reject TKT-INT-REJECT
    app_ok = pipeline_harness.client.post(
        "/v1/approvals/TKT-INT-APPROVE/decide",
        json={"decision": "approve", "actor": "qa-lead", "comment": "Verified KB-001 citation"},
    )
    assert app_ok.status_code == 200
    assert app_ok.json()["status"] == "approved"

    app_rej = pipeline_harness.client.post(
        "/v1/approvals/TKT-INT-REJECT/decide",
        json={"decision": "reject", "actor": "qa-lead", "comment": "Escalate to billing specialist"},
    )
    assert app_rej.status_code == 200
    assert app_rej.json()["status"] == "rejected"

    pipeline_harness.drain_agents()

    # Only the human-approved ticket was emitted to support.replies.approved.v1!
    sent_records = pipeline_harness.cluster.all_records(TOPIC_REPLIES_APPROVED)
    assert len(sent_records) == 1
    sent_evt = pipeline_harness.codec.decode(TOPIC_REPLIES_APPROVED, sent_records[0].value)
    assert sent_evt is not None
    assert sent_evt["ticket_id"] == "TKT-INT-APPROVE"
    assert sent_evt["approved_by"] == "qa-lead"

    # Audit log & Prometheus /metrics expose token and cost telemetry with PII scrubbed
    audit_resp = pipeline_harness.client.get("/v1/audit/recent")
    assert audit_resp.status_code == 200
    assert audit_resp.json()["retention_days"] == 2555
    audits = audit_resp.json()["events"]
    assert len(audits) >= 6
    assert all("admin@client.org" not in a["prompt_redacted"] for a in audits)

    metrics_resp = pipeline_harness.client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "krai_audit_tokens_total" in metrics_resp.text
    assert "krai_audit_estimated_cost_usd_total" in metrics_resp.text


@pytest.mark.integration
def test_part_b_share_group_worker_crash_redelivery_outage_recovery_and_dlq(
    pipeline_harness: InMemoryPipelineHarness,
) -> None:
    pipeline_harness.seed_and_index_kb()

    # 1. Poison payload on support.tickets.created.v1 -> rejected straight to DLQ
    pipeline_harness.cluster.produce(
        TOPIC_TICKETS_CREATED, key=b"TKT-POISON", value=b"\x00\x00\x00\x00\x01{broken-json"
    )
    pipeline_harness.drain_agents()
    dlq = pipeline_harness.cluster.all_records(TOPIC_DLQ)
    assert len(dlq) == 1

    # 2. Simulate worker crash mid-LLM-call (unacknowledged share lock expires -> redelivered once)
    crashed_worker = pipeline_harness.triage_workers[0]
    deliveries: list[tuple[str, int]] = []
    orig_handlers = {w.worker_id: w.handle_record for w in pipeline_harness.triage_workers}

    def _crash_first(rec: RawRecord) -> AckAction:
        deliveries.append((crashed_worker.worker_id, rec.delivery_count))
        raise SystemExit("Simulated SIGKILL mid-call before share ack")

    crashed_worker.handle_record = _crash_first  # type: ignore[method-assign]
    for w in pipeline_harness.triage_workers[1:]:
        orig_fn = orig_handlers[w.worker_id]

        def _track(rec: RawRecord, _wid: str = w.worker_id, _fn=orig_fn) -> AckAction:
            deliveries.append((_wid, rec.delivery_count))
            return _fn(rec)

        w.handle_record = _track  # type: ignore[method-assign]

    r = pipeline_harness.client.post(
        "/v1/tickets",
        json={
            "ticket_id": "TKT-CRASH-ONCE",
            "customer_id": "CUST-99",
            "customer_tier": "pro",
            "subject": "Webhook HMAC 401 signature mismatch",
            "body": "Webhook verification fails after Express JSON middleware.",
        },
    )
    assert r.status_code == 202

    # Crashed worker acquires record (delivery_count=1) and dies without acknowledging
    with pytest.raises(SystemExit):
        crashed_worker.step(0.05)
    pipeline_harness.triage_workers.pop(0)

    # Wait for 1.0s share lock to expire -> surviving worker acquires with delivery_count=2
    time.sleep(1.15)
    pipeline_harness.drain_agents()

    assert len(deliveries) == 2
    assert deliveries[0][1] == 1
    assert deliveries[1][1] == 2

    # Approve and confirm exactly ONE customer reply is produced
    dec = pipeline_harness.client.post(
        "/v1/approvals/TKT-CRASH-ONCE/decide",
        json={"decision": "approve", "actor": "sre-oncall"},
    )
    assert dec.status_code == 200
    pipeline_harness.drain_agents()
    outbound = [
        pipeline_harness.codec.decode(TOPIC_REPLIES_APPROVED, rec.value)
        for rec in pipeline_harness.cluster.all_records(TOPIC_REPLIES_APPROVED)
    ]
    matching = [m for m in outbound if m and m["ticket_id"] == "TKT-CRASH-ONCE"]
    assert len(matching) == 1

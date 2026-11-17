"""Failure Demo 1 (`make demo-outage`): Take the LLM provider down for two minutes — tickets queue, retry, and none are lost.

EXPECTED OUTPUT:
  [STEP 1] Stopping LLM provider (ollama) for 120s...
  [STEP 2] Submitting 6 customer support tickets while LLM provider is offline...
  [STEP 3] Holding 120s outage window — workers back off, release unacked leases, and open health circuit...
           Outage progress: elapsed=30s/120s | approvals_completed=0/6 | dlq_count=0
           Outage progress: elapsed=120s/120s | approvals_completed=0/6 | dlq_count=0
  [STEP 4] Restoring LLM provider (ollama)...
  [ASSERT PASS] 6/6 queued tickets recovered, triaged, drafted, and reviewed; 0 tickets lost.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def _build_tickets(batch_prefix: str, count: int = 6) -> list[dict[str, str]]:
    topics = [
        ("auth_sso", "Okta SAML_ASSERTION_EXPIRED after NTP drift", "Users see SAML_ASSERTION_EXPIRED on /auth/saml/acs."),
        ("api_webhooks", "Rotating webhook HMAC secret with zero downtime", "How do we rotate X-Webhook-Signature secret safely?"),
        ("rate_limits", "HTTP 429 Retry-After handling for Pro tier", "Workers hit HTTP 429 burst throttling; what backoff applies?"),
        ("billing_refund", "EU VAT reverse-charge credit note request", "Forgot VAT ID on invoice INV-2026-8841 5 days ago."),
        ("security_privacy", "GDPR Article 17 right to erasure SLA", "Customer submitted verified DSAR deletion request."),
        ("security_privacy", "Leaked sk-live token emergency revocation", "Developer exposed API key in public repo; need immediate revoke."),
    ]
    out: list[dict[str, str]] = []
    for i in range(count):
        _, subj, body = topics[i % len(topics)]
        out.append(
            {
                "ticket_id": f"TKT-OUTAGE-{batch_prefix}-{i + 1}",
                "customer_id": f"cust-outage-{i + 1}",
                "customer_email": f"ops{i + 1}@customer.example",
                "account_tier": "enterprise" if i % 2 == 0 else "pro",
                "subject": subj,
                "body": body,
            }
        )
    return out


def run_docker_mode(api_url: str, outage_seconds: float, ticket_count: int) -> None:
    prefix = uuid.uuid4().hex[:6].upper()
    tickets = _build_tickets(prefix, count=ticket_count)
    expected_ids = {t["ticket_id"] for t in tickets}

    print(f"[STEP 1] Stopping LLM provider container (ollama) for {outage_seconds:.0f}s...")
    subprocess.run(["docker", "compose", "stop", "ollama"], check=True)

    try:
        with httpx.Client(base_url=api_url, timeout=20.0) as client:
            print(f"[STEP 2] Submitting {len(tickets)} customer support tickets while LLM provider is offline...")
            for t in tickets:
                r = client.post("/v1/tickets", json=t)
                assert r.status_code == 202, r.text

            print(
                f"[STEP 3] Holding {outage_seconds:.0f}s outage window — workers back off, "
                "release unacked leases, and probe provider health..."
            )
            t0 = time.monotonic()
            next_log = 15.0
            while True:
                elapsed = time.monotonic() - t0
                if elapsed >= outage_seconds:
                    break
                if elapsed >= next_log:
                    apps = client.get("/v1/approvals").json()["items"]
                    done_now = sum(1 for a in apps if a["ticket_id"] in expected_ids)
                    print(
                        f"         Outage progress: elapsed={elapsed:.0f}s/{outage_seconds:.0f}s "
                        f"| approvals_completed={done_now}/{len(expected_ids)}"
                    )
                    next_log += 30.0
                time.sleep(1.0)
    finally:
        print("[STEP 4] Restoring LLM provider container (ollama)...")
        subprocess.run(["docker", "compose", "start", "ollama"], check=True)

    with httpx.Client(base_url=api_url, timeout=20.0) as client:
        deadline = time.monotonic() + 150.0
        recovered: set[str] = set()
        while time.monotonic() < deadline:
            apps = client.get("/v1/approvals").json()["items"]
            recovered = {a["ticket_id"] for a in apps if a["ticket_id"] in expected_ids}
            if len(recovered) == len(expected_ids):
                break
            time.sleep(1.0)

        assert recovered == expected_ids, (
            f"Ticket loss detected! Expected {expected_ids}, recovered {recovered}"
        )
        print(
            f"[ASSERT PASS] {len(recovered)}/{len(expected_ids)} queued tickets recovered, "
            "triaged, drafted, and reviewed; 0 tickets lost."
        )


def run_inmemory_mode(outage_seconds: float, ticket_count: int) -> None:
    from realtime_ai.kafka.topics import TOPIC_DLQ
    from scripts._harness import InMemoryPipelineHarness

    harness = InMemoryPipelineHarness()
    try:
        harness.seed_and_index_kb()
        prefix = uuid.uuid4().hex[:6].upper()
        tickets = _build_tickets(prefix, count=ticket_count)
        expected_ids = {t["ticket_id"] for t in tickets}

        print(f"[STEP 1] Activating simulated LLM provider outage for {outage_seconds:.1f}s...")
        harness.backend.outage_active = True

        print(f"[STEP 2] Submitting {len(tickets)} customer support tickets while LLM provider is offline...")
        for t in tickets:
            r = harness.client.post("/v1/tickets", json=t)
            assert r.status_code == 202

        print(f"[STEP 3] Stepping workers across {outage_seconds:.1f}s outage window...")
        t0 = time.monotonic()
        while time.monotonic() - t0 < outage_seconds:
            for w in harness.triage_workers:
                w.step(0.01)
            time.sleep(min(0.25, max(0.02, outage_seconds / 10.0)))

        apps_mid = harness.client.get("/v1/approvals").json()["items"]
        done_mid = [a for a in apps_mid if a["ticket_id"] in expected_ids]
        dlq_mid = len(harness.cluster.all_records(TOPIC_DLQ))
        assert len(done_mid) == 0, "Tickets should wait safely while LLM is down"
        assert dlq_mid == 0, f"Transient outage should not route valid tickets to DLQ (got {dlq_mid})"
        print(
            f"         Outage window complete ({outage_seconds:.1f}s): "
            f"approvals_completed=0/{len(expected_ids)} | dlq_count=0"
        )

        print("[STEP 4] Restoring LLM provider and draining queued tickets...")
        harness.backend.outage_active = False
        deadline = time.monotonic() + 15.0
        recovered: set[str] = set()
        while time.monotonic() < deadline:
            harness.drain_agents(max_rounds=10)
            apps = harness.client.get("/v1/approvals").json()["items"]
            recovered = {a["ticket_id"] for a in apps if a["ticket_id"] in expected_ids}
            if len(recovered) == len(expected_ids):
                break
            time.sleep(0.1)

        dlq_after = len(harness.cluster.all_records(TOPIC_DLQ))
        assert recovered == expected_ids, f"Missing tickets: {expected_ids - recovered}"
        assert dlq_after == 0, f"Expected 0 DLQ events, got {dlq_after}"
        print(
            f"[ASSERT PASS] {len(recovered)}/{len(expected_ids)} queued tickets recovered, "
            "triaged, drafted, and reviewed; 0 tickets lost."
        )
    finally:
        harness.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--outage-seconds", type=float, default=120.0)
    parser.add_argument("--tickets", type=int, default=6)
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()

    if args.in_memory:
        run_inmemory_mode(args.outage_seconds, args.tickets)
    else:
        run_docker_mode(args.api_url, args.outage_seconds, args.tickets)


if __name__ == "__main__":
    main()

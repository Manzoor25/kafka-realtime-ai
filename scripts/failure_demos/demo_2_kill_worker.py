"""Failure Demo 2 (`make demo-kill-worker`): Kill an agent worker mid-call — ticket is redelivered and processed once from the customer's point of view.

EXPECTED OUTPUT:
  [STEP 1] Submitting slow triage ticket TKT-KILL-<id> to KIP-932 share group sg-triage-workers...
  [STEP 2] Detected worker triage-worker-X mid-LLM-call (delivery_count=1); sending SIGKILL before ack!
  [STEP 3] Waiting for KIP-932 acquisition lock expiry and redelivery to surviving worker (delivery_count=2)...
  [STEP 4] Approving queued draft and verifying customer-facing output...
  [ASSERT PASS] Ticket redelivered after mid-call kill and processed ONCE from customer POV (approval_entries=1, sent_replies=1).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

_WORKER_NUM_RE = re.compile(r"triage-worker-(\d+)")


def run_docker_mode(api_url: str) -> None:
    ticket_id = f"TKT-KILL-{uuid.uuid4().hex[:6].upper()}"
    containers = [f"krai-agent-triage-{i}" for i in range(1, 5)]
    killed_container: str | None = None

    try:
        with httpx.Client(base_url=api_url, timeout=20.0) as client:
            print(f"[STEP 1] Submitting slow triage ticket {ticket_id} to KIP-932 share group sg-triage-workers...")
            resp = client.post(
                "/v1/tickets",
                json={
                    "ticket_id": ticket_id,
                    "customer_id": "cust-sigkill-demo",
                    "customer_email": "cto@sigkill-demo.example",
                    "account_tier": "enterprise",
                    "subject": "SAML_ASSERTION_EXPIRED after IdP clock drift",
                    "body": "[SIMULATE_SLOW_CALL_8S] Employees hit SAML_ASSERTION_EXPIRED at /auth/saml/acs.",
                },
            )
            assert resp.status_code == 202, resp.text

            # Detect which of the 4 share-group workers acquired the ticket mid-call
            detect_deadline = time.monotonic() + 15.0
            while time.monotonic() < detect_deadline and killed_container is None:
                for cname in containers:
                    logs = subprocess.run(
                        ["docker", "logs", "--since", "20s", cname],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    combined = (logs.stdout or "") + "\n" + (logs.stderr or "")
                    if "MID_CALL_IN_PROGRESS" in combined and ticket_id in combined:
                        killed_container = cname
                        break
                if killed_container is None:
                    time.sleep(0.15)

            assert killed_container is not None, f"Did not observe mid-call log for {ticket_id}"
            print(
                f"[STEP 2] Detected container {killed_container} mid-LLM-call (delivery_count=1); "
                "sending SIGKILL before ack!"
            )
            subprocess.run(["docker", "kill", "-s", "KILL", killed_container], check=True)

            print(
                "[STEP 3] Waiting for KIP-932 acquisition lock expiry (20s) and redelivery "
                "to a surviving triage worker..."
            )
            redelivery_deadline = time.monotonic() + 90.0
            approval_record = None
            while time.monotonic() < redelivery_deadline:
                r = client.get(f"/v1/approvals/{ticket_id}")
                if r.status_code == 200:
                    approval_record = r.json()["approval"]
                    break
                time.sleep(1.0)

            assert approval_record is not None, f"Ticket {ticket_id} was not redelivered after worker SIGKILL"

            # Confirm redelivery count >= 2 in surviving worker logs
            observed_redelivery = False
            for cname in containers:
                if cname == killed_container:
                    continue
                logs = subprocess.run(
                    ["docker", "logs", "--since", "90s", cname],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                combined = (logs.stdout or "") + "\n" + (logs.stderr or "")
                if ticket_id in combined and "delivery=2" in combined:
                    observed_redelivery = True
                    break

            print("[STEP 4] Approving queued draft and verifying customer-facing output...")
            dec = client.post(
                f"/v1/approvals/{ticket_id}/decide",
                json={"decision": "approve", "actor": "incident-commander"},
            )
            assert dec.status_code == 200, dec.text

            all_apps = [
                a for a in client.get("/v1/approvals").json()["items"] if a["ticket_id"] == ticket_id
            ]
            detail = client.get(f"/v1/approvals/{ticket_id}").json()
            sent_replies = detail["sent_replies"]

            assert len(all_apps) == 1, f"Expected 1 approval entry, got {len(all_apps)}"
            assert len(sent_replies) == 1, f"Expected 1 customer reply, got {len(sent_replies)}"
            print(
                f"[ASSERT PASS] Ticket {ticket_id} redelivered after mid-call SIGKILL "
                f"(delivery_2_logged={observed_redelivery}) and processed ONCE from customer POV "
                f"(approval_entries={len(all_apps)}, sent_replies={len(sent_replies)})."
            )
    finally:
        if killed_container is not None:
            print(f"[CLEANUP] Restarting killed worker container {killed_container}...")
            subprocess.run(["docker", "start", killed_container], check=False)


def run_inmemory_mode() -> None:
    from scripts._harness import InMemoryPipelineHarness

    class SimulatedSigKill(BaseException):
        """Simulates abrupt worker process termination mid-LLM-call before ShareConsumer ack."""

    harness = InMemoryPipelineHarness(lock_duration_seconds=0.8)
    try:
        harness.seed_and_index_kb()
        ticket_id = f"TKT-KILL-{uuid.uuid4().hex[:6].upper()}"
        killed_worker_id: str | None = None
        observed_deliveries: list[tuple[str, int]] = []

        def _crash_first_attempt(ticket: object, record: object) -> None:
            nonlocal killed_worker_id
            dc = getattr(record, "delivery_count", 1)
            observed_deliveries.append(("triage-worker-1", dc))
            if dc == 1 and killed_worker_id is None:
                killed_worker_id = "triage-worker-1"
                raise SimulatedSigKill("SIGKILL mid-LLM-call before acknowledge!")

        harness.triage_workers[0].mid_call_hook = _crash_first_attempt  # type: ignore[assignment]
        for idx, w in enumerate(harness.triage_workers[1:], start=2):
            wid = f"triage-worker-{idx}"
            w.mid_call_hook = lambda _t, rec, _w=wid: observed_deliveries.append(  # type: ignore[misc]
                (_w, rec.delivery_count)
            )

        print(f"[STEP 1] Submitting ticket {ticket_id} to KIP-932 share group sg-triage-workers...")
        resp = harness.client.post(
            "/v1/tickets",
            json={
                "ticket_id": ticket_id,
                "customer_id": "cust-sigkill-demo",
                "customer_email": "cto@sigkill-demo.example",
                "account_tier": "enterprise",
                "subject": "SAML_ASSERTION_EXPIRED after IdP clock drift",
                "body": "Employees hit SAML_ASSERTION_EXPIRED at /auth/saml/acs.",
            },
        )
        assert resp.status_code == 202

        try:
            harness.triage_workers[0].step(0.05)
        except SimulatedSigKill:
            print(
                f"[STEP 2] Worker {killed_worker_id} terminated mid-LLM-call (delivery_count=1) "
                "before acknowledging!"
            )

        print("[STEP 3] Waiting 0.9s for share-group acquisition lock expiry and stepping surviving workers...")
        time.sleep(0.95)
        # Remove dead worker-1 from rotation; surviving workers 2..4 pick up expired lease
        harness.triage_workers = harness.triage_workers[1:]
        harness.drain_agents(max_rounds=20)

        print("[STEP 4] Approving queued draft and verifying customer-facing output...")
        dec = harness.client.post(
            f"/v1/approvals/{ticket_id}/decide",
            json={"decision": "approve", "actor": "incident-commander"},
        )
        assert dec.status_code == 200, dec.text

        all_apps = [
            a for a in harness.client.get("/v1/approvals").json()["items"] if a["ticket_id"] == ticket_id
        ]
        detail = harness.client.get(f"/v1/approvals/{ticket_id}").json()
        sent_replies = detail["sent_replies"]

        assert any(dc == 2 for _, dc in observed_deliveries), f"Expected delivery_count=2, got {observed_deliveries}"
        assert len(all_apps) == 1
        assert len(sent_replies) == 1
        print(
            f"[ASSERT PASS] Ticket {ticket_id} redelivered ({observed_deliveries}) and processed "
            f"ONCE from customer POV (approval_entries={len(all_apps)}, sent_replies={len(sent_replies)})."
        )
    finally:
        harness.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()

    if args.in_memory:
        run_inmemory_mode()
    else:
        run_docker_mode(args.api_url)


if __name__ == "__main__":
    main()

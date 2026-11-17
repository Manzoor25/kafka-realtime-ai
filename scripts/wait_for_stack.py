"""Waits for the Docker Compose stack (`api`, `qdrant`, and initial KB seed indexing) to report healthy."""

from __future__ import annotations

import argparse
import sys
import time

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--min-kb-samples", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=240.0)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    last_status = "unreachable"
    print(f"Waiting up to {args.timeout:.0f}s for stack at {args.api_url} and initial KB indexing...")

    with httpx.Client(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.get(f"{args.api_url}/healthz")
                if resp.status_code == 200:
                    data = resp.json()
                    freshness = data.get("freshness") or {}
                    samples = int(freshness.get("samples_observed") or 0)
                    idx = data.get("active_index") or {}
                    last_status = (
                        f"api=ok collection={idx.get('collection_name')} "
                        f"indexed_docs={samples}/{args.min_kb_samples}"
                    )
                    if samples >= args.min_kb_samples:
                        print(
                            f"[READY] {last_status} | "
                            f"p50_freshness={freshness.get('p50_freshness_ms')}ms "
                            f"p95_freshness={freshness.get('p95_freshness_ms')}ms"
                        )
                        return
            except Exception as exc:
                last_status = str(exc)
            time.sleep(2.0)

    print(f"[TIMEOUT] Stack did not finish readiness check: {last_status}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()

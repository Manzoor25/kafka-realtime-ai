"""Shared pytest fixtures for unit, integration, and Docker Compose E2E tests."""

from __future__ import annotations

import sys
from collections.abc import Generator
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from realtime_ai.config import Settings  # noqa: E402
from scripts._harness import InMemoryPipelineHarness  # noqa: E402


@pytest.fixture()
def schemas_dir() -> Path:
    return ROOT_DIR / "schemas"


@pytest.fixture()
def inmemory_settings(tmp_path: Path, schemas_dir: Path) -> Settings:
    watch_dir = tmp_path / "watched_kb"
    watch_dir.mkdir(parents=True, exist_ok=True)
    return Settings(
        kafka_bootstrap_servers=f"memory://pytest-{tmp_path.name}",
        schema_registry_url="memory://sr",
        kafka_consumer_mode="share",
        share_group_triage="sg-triage-workers",
        qdrant_local_path=":memory:",
        qdrant_active_alias="kb_chunks_active",
        ai_provider="local",
        embedding_model="local-hash-v1",
        embedding_version="v1",
        embedding_dimensions=384,
        chat_model="local-support-v1",
        llm_timeout_seconds=4.0,
        llm_max_retries=2,
        llm_backoff_base_seconds=0.05,
        llm_backoff_max_seconds=0.2,
        llm_rate_limit_rps=50.0,
        watch_dir=watch_dir,
        schemas_dir=schemas_dir,
        seed_kb_dir=ROOT_DIR / "data" / "kb",
    )


@pytest.fixture()
def pipeline_harness() -> Generator[InMemoryPipelineHarness, None, None]:
    harness = InMemoryPipelineHarness(triage_worker_count=4, lock_duration_seconds=1.0)
    try:
        yield harness
    finally:
        harness.close()

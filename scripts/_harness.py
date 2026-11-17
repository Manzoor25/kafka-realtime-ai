"""Shared in-process pipeline harness for hermetic integration tests, evaluation, and offline failure demos."""

from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from realtime_ai.agents.drafter import DraftingAgent
from realtime_ai.agents.reviewer import ReviewerAgent
from realtime_ai.agents.tools import ToolExecutorAgent
from realtime_ai.agents.triage import TriageAgent
from realtime_ai.api.app import create_app
from realtime_ai.audit_exporter import AuditCostExporter
from realtime_ai.config import Settings
from realtime_ai.kafka.admin import ensure_topics_and_schemas
from realtime_ai.kafka.inmemory import get_inmemory_cluster
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_KB_INDEX_CONTROL
from realtime_ai.pipeline.indexer import StreamingIndexerStage
from realtime_ai.pipeline.redactor import PIIRedactionStage
from realtime_ai.pipeline.vector_store import QdrantVectorRepository
from realtime_ai.pipeline.watcher import FileWatcherSource, seed_watch_directory
from realtime_ai.providers.local_cpu import LocalCPUProvider
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import KbIndexControlEvent
from realtime_ai.schemas.registry import SchemaRegistryCodec


class InMemoryPipelineHarness:
    """Runs the complete Part A + Part B Kafka pipeline against in-memory Kafka + in-memory Qdrant."""

    def __init__(
        self,
        *,
        consumer_mode: str = "share",
        triage_worker_count: int = 4,
        lock_duration_seconds: float = 1.5,
        llm_max_retries: int = 2,
        llm_backoff_base_seconds: float = 0.05,
        provider_outage_probe_seconds: float = 0.25,
    ) -> None:
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="krai_harness_"))
        self.watch_dir = self._tmp_dir / "watched_kb"
        cluster_uri = f"memory://cluster-{uuid.uuid4().hex[:8]}"
        self.cluster = get_inmemory_cluster(cluster_uri)
        self.cluster.lock_duration_seconds = lock_duration_seconds

        repo_root = Path(__file__).resolve().parents[1]
        self.settings = Settings(
            kafka_bootstrap_servers=cluster_uri,
            schema_registry_url="memory://sr",
            kafka_consumer_mode=consumer_mode,  # type: ignore[arg-type]
            share_group_triage="sg-triage-workers",
            worker_id="harness",
            schemas_dir=repo_root / "schemas",
            seed_kb_dir=repo_root / "data" / "kb",
            watch_dir=self.watch_dir,
            qdrant_local_path=":memory:",
            ai_provider="local",
            embedding_model="local-hash-v1",
            embedding_version="v1",
            embedding_dimensions=384,
            chat_model="local-support-v1",
            llm_timeout_seconds=1.5,
            llm_max_retries=llm_max_retries,
            llm_backoff_base_seconds=llm_backoff_base_seconds,
            llm_backoff_max_seconds=0.25,
            llm_rate_limit_rps=100.0,
            provider_outage_probe_seconds=provider_outage_probe_seconds,
        )

        self.codec = SchemaRegistryCodec(
            self.settings.schema_registry_url, self.settings.schemas_dir
        )
        ensure_topics_and_schemas(self.settings, self.codec)
        self.producer = EventProducer(
            self.settings.kafka_bootstrap_servers, self.codec, client_id="harness-producer"
        )
        self.backend = LocalCPUProvider()
        self.provider = ResilientAIProvider(
            backend=self.backend, settings=self.settings, audit_producer=self.producer
        )
        self.vector_repo = QdrantVectorRepository(
            local_path=":memory:", active_alias=self.settings.qdrant_active_alias
        )

        initial_col = self.settings.collection_name_for(
            self.settings.embedding_model, self.settings.embedding_version
        )
        snap = self.vector_repo.ensure_collection(
            collection_name=initial_col,
            dimensions=self.settings.embedding_dimensions,
            embedding_model=self.settings.embedding_model,
            embedding_version=self.settings.embedding_version,
            point_alias=True,
        )
        self.producer.produce_event(
            TOPIC_KB_INDEX_CONTROL,
            key=snap.alias_name,
            payload=KbIndexControlEvent(
                alias_name=snap.alias_name,
                collection_name=snap.collection_name,
                embedding_model=snap.embedding_model,
                embedding_version=snap.embedding_version,
                dimensions=snap.dimensions,
                reason="harness_init",
            ),
            flush=True,
        )

        self.watcher = FileWatcherSource(self.watch_dir, self.producer)
        self.redactor = PIIRedactionStage(
            settings=self.settings, codec=self.codec, producer=self.producer
        )
        self.indexer = StreamingIndexerStage(
            settings=self.settings,
            codec=self.codec,
            producer=self.producer,
            provider=self.provider,
            vector_repo=self.vector_repo,
        )
        self.triage_workers = [
            TriageAgent(
                settings=self.settings,
                codec=self.codec,
                producer=self.producer,
                provider=self.provider,
                worker_id=f"triage-worker-{i + 1}",
            )
            for i in range(triage_worker_count)
        ]
        self.tools = ToolExecutorAgent(
            settings=self.settings,
            codec=self.codec,
            producer=self.producer,
            provider=self.provider,
            vector_repo=self.vector_repo,
        )
        self.drafter = DraftingAgent(
            settings=self.settings,
            codec=self.codec,
            producer=self.producer,
            provider=self.provider,
        )
        self.reviewer = ReviewerAgent(
            settings=self.settings,
            codec=self.codec,
            producer=self.producer,
            provider=self.provider,
        )
        self.audit_exporter = AuditCostExporter(settings=self.settings, codec=self.codec)

        self.app = create_app(
            settings=self.settings,
            codec=self.codec,
            producer=self.producer,
            provider=self.provider,
            vector_repo=self.vector_repo,
            start_background_poller=False,
        )
        self.client = TestClient(self.app)

    def seed_and_index_kb(self) -> int:
        seeded = seed_watch_directory(self.settings.seed_kb_dir, self.watch_dir)
        self.watcher.scan_once()
        self.drain_rag()
        return seeded

    def drain_rag(self, max_rounds: int = 25) -> int:
        work = 0
        for _ in range(max_rounds):
            step_work = (
                self.redactor.step(0.01)
                + self.indexer.step(0.01)
                + self.audit_exporter.step(0.01)
            )
            work += step_work
            if step_work == 0:
                break
        self.app.state.materializer.sync_once()
        return work

    def drain_agents(self, max_rounds: int = 30) -> int:
        work = 0
        for idx in range(max_rounds):
            step_work = 0
            # Round-robin across all 4 share-group workers (> 2 partitions)
            for offset in range(len(self.triage_workers)):
                worker = self.triage_workers[(idx + offset) % len(self.triage_workers)]
                step_work += worker.step(0.01)
            step_work += self.tools.step(0.01)
            step_work += self.drafter.step(0.01)
            step_work += self.reviewer.step(0.01)
            step_work += self.audit_exporter.step(0.01)
            work += step_work
            if step_work == 0:
                break
        self.app.state.materializer.sync_once()
        return work

    def close(self) -> None:
        self.client.close()
        self.redactor.close()
        self.indexer.close()
        for w in self.triage_workers:
            w.close()
        self.tools.close()
        self.drafter.close()
        self.reviewer.close()
        self.audit_exporter.close()
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

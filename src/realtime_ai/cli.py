"""Unified CLI entrypoint launching bootstrap, streaming RAG stages, autonomous agents, audit exporter, and FastAPI."""

from __future__ import annotations

import argparse
import json
import logging
import sys

import uvicorn

from realtime_ai.agents.drafter import DraftingAgent
from realtime_ai.agents.reviewer import ReviewerAgent
from realtime_ai.agents.tools import ToolExecutorAgent
from realtime_ai.agents.triage import TriageAgent
from realtime_ai.api.app import create_app
from realtime_ai.audit_exporter import AuditCostExporter
from realtime_ai.config import Settings
from realtime_ai.kafka.admin import ensure_topics_and_schemas
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_KB_INDEX_CONTROL
from realtime_ai.metrics import maybe_start_metrics_server
from realtime_ai.pipeline.indexer import StreamingIndexerStage
from realtime_ai.pipeline.redactor import PIIRedactionStage
from realtime_ai.pipeline.reembed import replay_and_switch_index
from realtime_ai.pipeline.vector_store import QdrantVectorRepository
from realtime_ai.pipeline.watcher import FileWatcherSource, seed_watch_directory
from realtime_ai.providers.factory import create_resilient_provider
from realtime_ai.schemas.models import KbIndexControlEvent
from realtime_ai.schemas.registry import SchemaRegistryCodec


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    )


def _build_common(settings: Settings) -> tuple[SchemaRegistryCodec, EventProducer]:
    codec = SchemaRegistryCodec(settings.schema_registry_url, settings.schemas_dir)
    producer = EventProducer(
        settings.kafka_bootstrap_servers,
        codec,
        client_id=f"krai-{settings.worker_id}",
    )
    return codec, producer


def run_bootstrap(settings: Settings) -> None:
    codec, producer = _build_common(settings)
    ensure_topics_and_schemas(settings, codec)

    vector_repo = QdrantVectorRepository(
        url=settings.qdrant_url,
        local_path=settings.qdrant_local_path,
        active_alias=settings.qdrant_active_alias,
    )
    vector_repo.wait_until_ready(timeout_seconds=60.0)

    # Probe active embedding model dimensions dynamically when possible
    provider = create_resilient_provider(settings, audit_producer=producer)
    try:
        probe = provider.embed(
            ["bootstrap dimension check"],
            model=settings.embedding_model,
            stage="bootstrap",
            agent_name="app-bootstrap",
            correlation_id="bootstrap",
        )
        dims = len(probe.vectors[0])
    except Exception:
        dims = settings.embedding_dimensions

    initial_collection = settings.collection_name_for(
        settings.embedding_model, settings.embedding_version
    )
    snap = vector_repo.ensure_collection(
        collection_name=initial_collection,
        dimensions=dims,
        embedding_model=settings.embedding_model,
        embedding_version=settings.embedding_version,
        point_alias=True,
    )
    ctrl_evt = KbIndexControlEvent(
        alias_name=snap.alias_name,
        collection_name=snap.collection_name,
        embedding_model=snap.embedding_model,
        embedding_version=snap.embedding_version,
        dimensions=snap.dimensions,
        reason="initial_bootstrap",
    )
    producer.produce_event(
        TOPIC_KB_INDEX_CONTROL,
        key=snap.alias_name,
        payload=ctrl_evt,
        flush=True,
    )
    seeded = seed_watch_directory(settings.seed_kb_dir, settings.watch_dir)
    logging.getLogger(__name__).info(
        "Bootstrap complete: alias '%s' -> '%s' (%s/%s, dim=%d); seeded %d KB file(s) into %s",
        snap.alias_name,
        snap.collection_name,
        snap.embedding_model,
        snap.embedding_version,
        snap.dimensions,
        seeded,
        settings.watch_dir,
    )


def main(argv: list[str] | None = None) -> None:
    _configure_logging()
    parser = argparse.ArgumentParser(prog="realtime-ai")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("bootstrap", help="Create topics, register JSON Schemas, init Qdrant alias, seed KB")
    sub.add_parser("watcher", help="Run file-system / CDC document change watcher")
    sub.add_parser("redactor", help="Run PII redaction stream stage")
    sub.add_parser("indexer", help="Run idempotent chunk + embed + Qdrant upsert stage")
    sub.add_parser("triage", help="Run KIP-932 share-group triage agent worker")
    sub.add_parser("tools", help="Run event-driven tool execution worker")
    sub.add_parser("drafter", help="Run ticket reply drafting agent")
    sub.add_parser("reviewer", help="Run compliance/grounding reviewer agent")
    sub.add_parser("audit", help="Run compliance audit & token cost Prometheus exporter")
    sub.add_parser("api", help="Run FastAPI QA + Human Approval web server")

    p_reembed = sub.add_parser("reembed", help="Replay sanitized log into a new model & switch alias")
    p_reembed.add_argument("--model", required=True, help="Target embedding model (e.g., nomic-embed-text:v1.5)")
    p_reembed.add_argument("--version", required=True, help="Target version tag (e.g., v2)")
    p_reembed.add_argument("--dimensions", type=int, default=None, help="Optional vector dimensions override")

    args = parser.parse_args(argv)
    settings = Settings()

    if args.command == "bootstrap":
        run_bootstrap(settings)
        return

    if args.command == "api":
        app = create_app(settings=settings)
        uvicorn.run(app, host=settings.api_host, port=settings.api_port, log_level="info")
        return

    maybe_start_metrics_server(settings.metrics_port)
    codec, producer = _build_common(settings)
    provider = create_resilient_provider(settings, audit_producer=producer)
    vector_repo = QdrantVectorRepository(
        url=settings.qdrant_url,
        local_path=settings.qdrant_local_path,
        active_alias=settings.qdrant_active_alias,
    )

    if args.command == "watcher":
        seed_watch_directory(settings.seed_kb_dir, settings.watch_dir)
        FileWatcherSource(settings.watch_dir, producer).run_forever(settings.watch_poll_seconds)
    elif args.command == "redactor":
        PIIRedactionStage(settings=settings, codec=codec, producer=producer).run_forever()
    elif args.command == "indexer":
        StreamingIndexerStage(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
            vector_repo=vector_repo,
        ).run_forever()
    elif args.command == "triage":
        TriageAgent(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
        ).run_forever()
    elif args.command == "tools":
        ToolExecutorAgent(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
            vector_repo=vector_repo,
        ).run_forever()
    elif args.command == "drafter":
        DraftingAgent(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
        ).run_forever()
    elif args.command == "reviewer":
        ReviewerAgent(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
        ).run_forever()
    elif args.command == "audit":
        AuditCostExporter(settings=settings, codec=codec).run_forever()
    elif args.command == "reembed":
        report = replay_and_switch_index(
            settings=settings,
            codec=codec,
            producer=producer,
            provider=provider,
            vector_repo=vector_repo,
            new_embedding_model=args.model,
            new_embedding_version=args.version,
            new_dimensions=args.dimensions,
        )
        sys.stdout.write(json.dumps(report.__dict__, indent=2) + "\n")


if __name__ == "__main__":
    main()

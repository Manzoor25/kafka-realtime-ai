"""Zero-downtime corpus re-embedding by replaying compacted Kafka log into a blue/green Qdrant index."""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass

from realtime_ai.config import Settings
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_KB_INDEX_CONTROL
from realtime_ai.pipeline.indexer import StreamingIndexerStage
from realtime_ai.pipeline.vector_store import ActiveIndexSnapshot, QdrantVectorRepository
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import KbIndexControlEvent
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplayCutoverReport:
    target_collection: str
    embedding_model: str
    embedding_version: str
    dimensions: int
    records_replayed: int
    points_in_new_index: int
    replay_duration_ms: float
    cutover_duration_ms: float


def replay_and_switch_index(
    *,
    settings: Settings,
    codec: SchemaRegistryCodec,
    producer: EventProducer,
    provider: ResilientAIProvider,
    vector_repo: QdrantVectorRepository,
    new_embedding_model: str,
    new_embedding_version: str,
    new_dimensions: int | None = None,
    idle_polls_to_catch_up: int = 4,
    reason: str = "blue_green_model_upgrade",
) -> ReplayCutoverReport:
    """Replays `kb.documents.sanitized.v1` from offset 0 into a second collection, then swaps alias atomically."""
    t_replay_start = time.perf_counter()

    # Probe actual vector dimensionality from the target model if not explicitly passed
    if new_dimensions is None:
        probe = provider.embed(
            ["dimension probe"],
            model=new_embedding_model,
            stage="reembed-probe",
            agent_name="reembed-controller",
            correlation_id="probe",
        )
        new_dimensions = len(probe.vectors[0])

    target_collection = settings.collection_name_for(new_embedding_model, new_embedding_version)
    vector_repo.ensure_collection(
        collection_name=target_collection,
        dimensions=new_dimensions,
        embedding_model=new_embedding_model,
        embedding_version=new_embedding_version,
        point_alias=False,  # Keep existing active alias serving live queries during replay!
    )

    replay_group = f"cg-kb-replay-{new_embedding_version}-{uuid.uuid4().hex[:6]}"
    replay_stage = StreamingIndexerStage(
        settings=settings,
        codec=codec,
        producer=producer,
        provider=provider,
        vector_repo=vector_repo,
        collection_name=target_collection,
        embedding_model=new_embedding_model,
        embedding_version=new_embedding_version,
        group_id=replay_group,
    )

    records_replayed = 0
    idle_streak = 0
    try:
        while idle_streak < idle_polls_to_catch_up:
            count = replay_stage.step(poll_timeout=0.4)
            if count > 0:
                records_replayed += count
                idle_streak = 0
            else:
                idle_streak += 1
    finally:
        replay_stage.close()

    replay_duration_ms = (time.perf_counter() - t_replay_start) * 1000.0

    # Atomic zero-downtime cutover: update Qdrant alias + publish compacted control event
    t_switch = time.perf_counter()
    snap: ActiveIndexSnapshot = vector_repo.switch_alias_atomic(
        target_collection=target_collection,
        embedding_model=new_embedding_model,
        embedding_version=new_embedding_version,
        dimensions=new_dimensions,
    )
    ctrl_evt = KbIndexControlEvent(
        alias_name=snap.alias_name,
        collection_name=snap.collection_name,
        embedding_model=snap.embedding_model,
        embedding_version=snap.embedding_version,
        dimensions=snap.dimensions,
        reason=reason,
    )
    producer.produce_event(
        TOPIC_KB_INDEX_CONTROL,
        key=snap.alias_name,
        payload=ctrl_evt,
        flush=True,
    )
    cutover_duration_ms = (time.perf_counter() - t_switch) * 1000.0
    total_points = vector_repo.count_points(target_collection)

    logger.info(
        "Completed zero-downtime replay & cutover to %s (%s/%s, dim=%d, points=%d) in %.1fms (cutover=%.2fms)",
        target_collection,
        new_embedding_model,
        new_embedding_version,
        new_dimensions,
        total_points,
        replay_duration_ms,
        cutover_duration_ms,
    )
    return ReplayCutoverReport(
        target_collection=target_collection,
        embedding_model=new_embedding_model,
        embedding_version=new_embedding_version,
        dimensions=new_dimensions,
        records_replayed=records_replayed,
        points_in_new_index=total_points,
        replay_duration_ms=round(replay_duration_ms, 2),
        cutover_duration_ms=round(cutover_duration_ms, 2),
    )

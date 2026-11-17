"""Canonical Kafka topic topology, partition counts, compaction, and compliance retention."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    cleanup_policy: str = "delete"
    retention_ms: int = 7 * 24 * 3600 * 1000  # 7 days default
    schema_file: str | None = None
    extra_config: dict[str, str] = field(default_factory=dict)


# Topic names
TOPIC_KB_RAW = "kb.documents.raw.v1"
TOPIC_KB_SANITIZED = "kb.documents.sanitized.v1"
TOPIC_KB_INDEX_CONTROL = "kb.index.control.v1"
TOPIC_KB_INDEX_TELEMETRY = "kb.index.telemetry.v1"

TOPIC_TICKETS_CREATED = "support.tickets.created.v1"
TOPIC_AGENT_TRIAGE = "support.agent.triage.v1"
TOPIC_AGENT_TOOL_CALLS = "support.agent.tool.calls.v1"
TOPIC_AGENT_TOOL_RESULTS = "support.agent.tool.results.v1"
TOPIC_AGENT_DRAFTS = "support.agent.drafts.v1"
TOPIC_AGENT_REVIEWS = "support.agent.reviews.v1"
TOPIC_HUMAN_APPROVALS = "support.human.approvals.v1"
TOPIC_REPLIES_APPROVED = "support.replies.approved.v1"

TOPIC_AI_AUDIT = "ai.audit.events.v1"
TOPIC_DLQ = "dlq.events.v1"

SEVEN_YEARS_MS = 2555 * 24 * 3600 * 1000  # 220,752,000,000 ms (~7 years for compliance)


ALL_TOPICS: tuple[TopicSpec, ...] = (
    # Part A: Streaming RAG
    TopicSpec(
        name=TOPIC_KB_RAW,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=7 * 24 * 3600 * 1000,
        schema_file="kb.documents.raw.v1.json",
    ),
    TopicSpec(
        # Log-compacted sanitized corpus: enables full corpus replay into new embedding models!
        name=TOPIC_KB_SANITIZED,
        partitions=3,
        cleanup_policy="compact",
        retention_ms=-1,
        schema_file="kb.documents.sanitized.v1.json",
        extra_config={
            "delete.retention.ms": str(24 * 3600 * 1000),  # retain tombstones 24h for replay consumers
            "min.cleanable.dirty.ratio": "0.05",
        },
    ),
    TopicSpec(
        name=TOPIC_KB_INDEX_CONTROL,
        partitions=1,
        cleanup_policy="compact",
        retention_ms=-1,
        schema_file="kb.index.control.v1.json",
    ),
    TopicSpec(
        name=TOPIC_KB_INDEX_TELEMETRY,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=7 * 24 * 3600 * 1000,
        schema_file="kb.index.telemetry.v1.json",
    ),
    # Part B: Event-Driven Multi-Agent Support Triage
    TopicSpec(
        # Intentionally 2 partitions consumed by 4 KIP-932 share-group triage workers (workers > partitions!)
        name=TOPIC_TICKETS_CREATED,
        partitions=2,
        cleanup_policy="delete",
        retention_ms=14 * 24 * 3600 * 1000,
        schema_file="support.tickets.created.v1.json",
    ),
    TopicSpec(
        name=TOPIC_AGENT_TRIAGE,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=14 * 24 * 3600 * 1000,
        schema_file="support.agent.triage.v1.json",
    ),
    TopicSpec(
        name=TOPIC_AGENT_TOOL_CALLS,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=7 * 24 * 3600 * 1000,
        schema_file="support.agent.tool.calls.v1.json",
    ),
    TopicSpec(
        name=TOPIC_AGENT_TOOL_RESULTS,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=7 * 24 * 3600 * 1000,
        schema_file="support.agent.tool.results.v1.json",
    ),
    TopicSpec(
        name=TOPIC_AGENT_DRAFTS,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=14 * 24 * 3600 * 1000,
        schema_file="support.agent.drafts.v1.json",
    ),
    TopicSpec(
        name=TOPIC_AGENT_REVIEWS,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=14 * 24 * 3600 * 1000,
        schema_file="support.agent.reviews.v1.json",
    ),
    TopicSpec(
        # Compacted state log keyed by ticket_id for durable Human-in-the-Loop approval queue
        name=TOPIC_HUMAN_APPROVALS,
        partitions=3,
        cleanup_policy="compact",
        retention_ms=-1,
        schema_file="support.human.approvals.v1.json",
    ),
    TopicSpec(
        name=TOPIC_REPLIES_APPROVED,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=30 * 24 * 3600 * 1000,
        schema_file="support.replies.approved.v1.json",
    ),
    # Governance, Audit & Resilience
    TopicSpec(
        # 7-year compliance retention for every prompt, response, model version, and token count
        name=TOPIC_AI_AUDIT,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=SEVEN_YEARS_MS,
        schema_file="ai.audit.events.v1.json",
    ),
    TopicSpec(
        name=TOPIC_DLQ,
        partitions=3,
        cleanup_policy="delete",
        retention_ms=30 * 24 * 3600 * 1000,
        schema_file="dlq.events.v1.json",
    ),
)

TOPICS_BY_NAME: dict[str, TopicSpec] = {spec.name: spec for spec in ALL_TOPICS}

"""Prometheus metrics registry and optional background HTTP exporter for workers."""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# --- Part A: Streaming RAG & Freshness ---
DOCS_WATCHED_TOTAL = Counter(
    "krai_kb_watcher_events_total",
    "Document change events emitted by file watcher / CDC webhook",
    ["op"],
)
PII_REDACTIONS_TOTAL = Counter(
    "krai_pii_redactions_total",
    "Total PII entities redacted before embedding",
    ["pii_type"],
)
PII_DOCS_PROCESSED_TOTAL = Counter(
    "krai_pii_documents_processed_total",
    "Documents processed by PII redaction stage",
    ["event_kind"],
)
CHUNKS_UPSERTED_TOTAL = Counter(
    "krai_vector_chunks_upserted_total",
    "Idempotent vector chunk upserts into Qdrant",
    ["collection", "embedding_model", "embedding_version"],
)
DOCS_DELETED_TOTAL = Counter(
    "krai_vector_documents_deleted_total",
    "Vector deletions triggered by Kafka tombstones",
    ["collection"],
)
KB_FRESHNESS_SECONDS = Histogram(
    "krai_kb_freshness_seconds",
    "Measured time from document change timestamp to vector retrievability in Qdrant",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.5, 5.0, 10.0, 30.0),
)
KB_LAST_FRESHNESS_MS = Gauge(
    "krai_kb_last_freshness_ms",
    "Most recent end-to-end document-change-to-retrievable latency in milliseconds",
)
QA_REQUESTS_TOTAL = Counter(
    "krai_qa_requests_total",
    "Question-answering endpoint calls",
    ["status", "embedding_version"],
)
QA_LATENCY_SECONDS = Histogram(
    "krai_qa_latency_seconds",
    "End-to-end QA endpoint latency in seconds",
    buckets=(0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
ACTIVE_EMBEDDING_INFO = Gauge(
    "krai_active_embedding_index_info",
    "Active Qdrant collection and embedding version serving live traffic",
    ["alias", "collection", "embedding_model", "embedding_version"],
)

# --- Part B: Event-Driven Agents, Resilience & Share Groups ---
AGENT_EVENTS_TOTAL = Counter(
    "krai_agent_events_total",
    "Events processed by autonomous support agents",
    ["agent", "outcome", "consumer_mode"],
)
SHARE_GROUP_DELIVERY_COUNT = Histogram(
    "krai_share_group_delivery_attempts",
    "Observed KIP-932 ShareConsumer delivery_count per processed message",
    ["group_id"],
    buckets=(1, 2, 3, 4, 5, 8, 10),
)
CONSUMER_PAUSED_GAUGE = Gauge(
    "krai_consumer_paused",
    "1 when consumer is paused due to LLM rate limiting or provider outage circuit open",
    ["worker_id", "reason"],
)
LLM_CALLS_TOTAL = Counter(
    "krai_llm_calls_total",
    "Total embedding and chat calls executed via ResilientAIProvider",
    ["provider", "model", "operation", "stage", "status"],
)
LLM_RETRIES_TOTAL = Counter(
    "krai_llm_retries_total",
    "Retries triggered on LLM/embedding timeouts, 429s, or transient provider outages",
    ["provider", "operation", "stage", "reason"],
)
LLM_CALL_LATENCY_SECONDS = Histogram(
    "krai_llm_call_latency_seconds",
    "Latency of individual provider calls in seconds",
    ["provider", "operation", "stage"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0),
)
DLQ_EVENTS_TOTAL = Counter(
    "krai_dlq_events_total",
    "Poison or exhausted records routed to dlq.events.v1",
    ["source_topic", "worker_stage", "error_type"],
)
HUMAN_APPROVALS_GAUGE = Gauge(
    "krai_human_approvals_by_status",
    "Current count of tickets in human approval state store by status",
    ["status"],
)

# --- Compliance Audit & Token Cost Dashboard Metrics (populated from ai.audit.events.v1) ---
AUDIT_RECORDS_CONSUMED_TOTAL = Counter(
    "krai_audit_records_total",
    "Audit records consumed from compliance topic ai.audit.events.v1",
    ["provider", "model_version", "stage", "operation", "status"],
)
AUDIT_TOKENS_TOTAL = Counter(
    "krai_audit_tokens_total",
    "Token consumption recorded on compliance audit topic",
    ["provider", "model_version", "stage", "token_type"],
)
AUDIT_ESTIMATED_COST_USD_TOTAL = Counter(
    "krai_audit_estimated_cost_usd_total",
    "Cumulative estimated LLM/embedding cost in USD derived from ai.audit.events.v1",
    ["provider", "model_version", "stage"],
)
AUDIT_STAGE_LATENCY_MS = Histogram(
    "krai_audit_stage_latency_ms",
    "Model latency distribution recorded on ai.audit.events.v1",
    ["stage", "model_version"],
    buckets=(5, 15, 50, 100, 250, 500, 1000, 2500, 5000, 15000),
)


def maybe_start_metrics_server(port: int) -> None:
    if port > 0:
        start_http_server(port)
        logger.info("Prometheus metrics HTTP exporter listening on 0.0.0.0:%d", port)

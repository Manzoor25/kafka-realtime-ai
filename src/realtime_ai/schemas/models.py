"""Typed Pydantic event models aligned 1:1 with the registered JSON Schemas."""

from __future__ import annotations

import time
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Citation(BaseModel):
    doc_id: str
    chunk_id: str
    title: str
    snippet: str = ""
    score: float
    embedding_model: str = ""
    embedding_version: str


class KbDocumentRawEvent(BaseModel):
    doc_id: str
    event_id: str = Field(default_factory=lambda: new_id("evt"))
    op: Literal["create", "update"]
    title: str
    content: str
    source_uri: str
    source_ts_ms: int = Field(default_factory=now_ms)


class KbDocumentSanitizedEvent(BaseModel):
    doc_id: str
    event_id: str
    title: str
    sanitized_content: str
    pii_redactions_count: int
    pii_types_found: list[str]
    source_uri: str
    source_ts_ms: int
    redacted_ts_ms: int = Field(default_factory=now_ms)


class KbIndexControlEvent(BaseModel):
    alias_name: str
    collection_name: str
    embedding_model: str
    embedding_version: str
    dimensions: int
    switched_at_ms: int = Field(default_factory=now_ms)
    reason: str


class KbIndexTelemetryEvent(BaseModel):
    doc_id: str
    event_id: str
    op: Literal["upsert", "delete"]
    collection_name: str
    embedding_model: str
    embedding_version: str
    chunks_upserted: int
    source_ts_ms: int
    indexed_ts_ms: int = Field(default_factory=now_ms)
    freshness_ms: float


class SupportTicketCreatedEvent(BaseModel):
    ticket_id: str = Field(default_factory=lambda: new_id("TKT"))
    customer_id: str
    customer_email: str
    account_tier: Literal["free", "pro", "enterprise"] = "pro"
    subject: str
    body: str
    created_ts_ms: int = Field(default_factory=now_ms)


class SupportAgentTriageEvent(BaseModel):
    ticket_id: str
    triage_id: str = Field(default_factory=lambda: new_id("tri"))
    customer_id: str
    account_tier: str
    subject: str
    ticket_body_redacted: str
    category: Literal[
        "auth_sso",
        "billing_refund",
        "api_webhooks",
        "rate_limits",
        "security_privacy",
        "general",
    ]
    urgency: Literal["p1_critical", "p2_high", "p3_normal", "p4_low"]
    sentiment: Literal["frustrated", "urgent", "neutral", "positive"]
    summary: str
    search_query: str
    requested_tools: list[str]
    worker_id: str
    delivery_count: int = 1
    model_version: str
    created_ts_ms: int = Field(default_factory=now_ms)


class SupportAgentToolCallEvent(BaseModel):
    call_id: str = Field(default_factory=lambda: new_id("call"))
    ticket_id: str
    requested_by_agent: str
    tool_name: Literal["kb_search", "account_sla_lookup", "billing_ledger_check"]
    arguments: dict[str, Any]
    total_calls_for_ticket: int = 1
    created_ts_ms: int = Field(default_factory=now_ms)


class SupportAgentToolResultEvent(BaseModel):
    call_id: str
    ticket_id: str
    tool_name: str
    status: Literal["ok", "error"]
    output: str
    citations: list[dict[str, Any]] = Field(default_factory=list)
    total_calls_for_ticket: int = 1
    latency_ms: float
    completed_ts_ms: int = Field(default_factory=now_ms)


class SupportAgentDraftEvent(BaseModel):
    draft_id: str = Field(default_factory=lambda: new_id("drf"))
    ticket_id: str
    customer_id: str
    subject: str
    ticket_body_redacted: str
    revision: int = 1
    category: str
    urgency: str
    reply_body: str
    citations: list[dict[str, Any]]
    tool_context_summary: str
    model_version: str
    created_ts_ms: int = Field(default_factory=now_ms)


class SupportAgentReviewEvent(BaseModel):
    review_id: str = Field(default_factory=lambda: new_id("rev"))
    draft_id: str
    ticket_id: str
    revision: int
    verdict: Literal["ready_for_human", "needs_revision"]
    quality_score: float
    policy_flags: list[str] = Field(default_factory=list)
    feedback: str
    draft_snapshot: dict[str, Any] = Field(default_factory=dict)
    model_version: str
    reviewed_ts_ms: int = Field(default_factory=now_ms)


class SupportHumanApprovalStateEvent(BaseModel):
    ticket_id: str
    draft_id: str
    revision: int
    status: Literal["pending", "approved", "rejected"]
    customer_id: str
    subject: str
    urgency: str
    category: str
    proposed_reply: str
    final_reply: str | None = None
    citations: list[dict[str, Any]] = Field(default_factory=list)
    reviewer_notes: str = ""
    human_actor: str | None = None
    human_comment: str | None = None
    queued_ts_ms: int = Field(default_factory=now_ms)
    decided_ts_ms: int | None = None


class SupportReplyApprovedEvent(BaseModel):
    reply_id: str = Field(default_factory=lambda: new_id("rpl"))
    ticket_id: str
    draft_id: str
    customer_id: str
    subject: str
    final_reply: str
    citations: list[dict[str, Any]] = Field(default_factory=list)
    approved_by: str
    sent_ts_ms: int = Field(default_factory=now_ms)


class AiAuditEvent(BaseModel):
    audit_id: str = Field(default_factory=lambda: new_id("aud"))
    correlation_id: str
    stage: str
    agent_name: str
    operation: Literal["embed", "chat"]
    provider: str
    model_version: str
    prompt_redacted: str
    response_text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_usd: float
    latency_ms: float
    retry_count: int = 0
    status: Literal["ok", "error"]
    timestamp_ms: int = Field(default_factory=now_ms)


class DeadLetterEvent(BaseModel):
    dlq_id: str = Field(default_factory=lambda: new_id("dlq"))
    source_topic: str
    source_partition: int
    source_offset: int
    message_key: str | None
    worker_stage: str
    error_type: str
    error_message: str
    retry_attempts: int
    raw_payload_b64: str
    failed_ts_ms: int = Field(default_factory=now_ms)

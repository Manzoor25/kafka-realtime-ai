"""Drafting agent correlating triage + tool results over Kafka topics and revising on reviewer feedback."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_AGENT_DRAFTS,
    TOPIC_AGENT_REVIEWS,
    TOPIC_AGENT_TOOL_RESULTS,
    TOPIC_AGENT_TRIAGE,
)
from realtime_ai.metrics import AGENT_EVENTS_TOTAL
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.providers.base import TransientProviderError
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    SupportAgentDraftEvent,
    SupportAgentReviewEvent,
    SupportAgentToolResultEvent,
    SupportAgentTriageEvent,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)


@dataclass
class _TicketDraftContext:
    triage: SupportAgentTriageEvent | None = None
    expected_tools: int = 1
    tool_results: dict[str, SupportAgentToolResultEvent] = field(default_factory=dict)
    initial_draft_emitted: bool = False
    highest_revision_emitted: int = 0


def _ensure_grounded_citation_and_pii_safety(
    reply_text: str, citations: list[dict[str, Any]]
) -> str:
    """Guarantees customer-facing drafts never leak PII and explicitly reference retrieved KB IDs."""
    clean = redact_pii(reply_text).sanitized_text.strip()
    if citations:
        top_doc = str(citations[0].get("doc_id", "")).strip()
        if top_doc and f"[{top_doc}]" not in clean:
            clean = f"{clean}\n\nReference: [{top_doc}] {citations[0].get('title', '')}".strip()
    return clean


class DraftingAgent:
    """Joins `support.agent.triage.v1` + `support.agent.tool.results.v1` by `ticket_id` and handles review revisions."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        provider: ResilientAIProvider,
        group_id: str = "cg-agent-drafter",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.provider = provider
        self._state: dict[str, _TicketDraftContext] = {}
        self._outage_circuit_open = False

        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_AGENT_TRIAGE, TOPIC_AGENT_TOOL_RESULTS, TOPIC_AGENT_REVIEWS],
            mode="classic",
            worker_id=f"{settings.worker_id}-drafter",
            auto_offset_reset="earliest",
        )

    def handle_record(self, record: RawRecord) -> AckAction:
        if record.value is None:
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(record.topic, record.value)
            assert decoded is not None
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="agent-drafter",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=1,
            )
            return AckAction.REJECT

        try:
            if record.topic == TOPIC_AGENT_TRIAGE:
                triage_evt = SupportAgentTriageEvent.model_validate(decoded)
                ctx = self._state.setdefault(triage_evt.ticket_id, _TicketDraftContext())
                # Deduplicate redelivered triage events (e.g., if a triage worker died right after producing)
                if ctx.triage is None:
                    ctx.triage = triage_evt
                    ctx.expected_tools = max(1, len(triage_evt.requested_tools))
                return self._maybe_emit_initial_draft(triage_evt.ticket_id)

            if record.topic == TOPIC_AGENT_TOOL_RESULTS:
                tool_res = SupportAgentToolResultEvent.model_validate(decoded)
                ctx = self._state.setdefault(tool_res.ticket_id, _TicketDraftContext())
                ctx.expected_tools = max(ctx.expected_tools, tool_res.total_calls_for_ticket)
                # Key by tool_name so duplicate tool executions on redelivery overwrite idempotently
                ctx.tool_results[tool_res.tool_name] = tool_res
                return self._maybe_emit_initial_draft(tool_res.ticket_id)

            if record.topic == TOPIC_AGENT_REVIEWS:
                review_evt = SupportAgentReviewEvent.model_validate(decoded)
                if (
                    review_evt.verdict == "needs_revision"
                    and review_evt.revision < self.settings.max_draft_revisions
                ):
                    return self._emit_revised_draft(review_evt)
                return AckAction.ACCEPT

        except TransientProviderError as exc:
            logger.warning("Drafter releasing record on %s due to transient LLM outage: %s", record.topic, exc)
            self._outage_circuit_open = True
            self.consumer.pause_for(self.settings.provider_outage_probe_seconds, reason="provider_outage")
            return AckAction.RELEASE

        return AckAction.ACCEPT

    def _maybe_emit_initial_draft(self, ticket_id: str) -> AckAction:
        ctx = self._state[ticket_id]
        if ctx.initial_draft_emitted or ctx.triage is None:
            return AckAction.ACCEPT
        if len(ctx.tool_results) < ctx.expected_tools:
            return AckAction.ACCEPT

        triage = ctx.triage
        citations: list[dict[str, Any]] = []
        tool_sections: list[str] = []
        for tname, tres in ctx.tool_results.items():
            tool_sections.append(f"[{tname}] {tres.output}")
            for c in tres.citations:
                if not any(existing.get("chunk_id") == c.get("chunk_id") for existing in citations):
                    citations.append(c)

        tool_summary = "\n\n".join(tool_sections)
        sys_prompt = (
            "You are a technical customer support specialist. Draft an accurate, empathetic reply "
            "grounded strictly in the retrieved Knowledge Base context. Cite relevant KB article IDs "
            "in brackets like [KB-001], and never include internal emails or raw secrets."
        )
        usr_prompt = (
            f"TICKET: {triage.ticket_id} (Tier: {triage.account_tier}, Urgency: {triage.urgency})\n"
            f"SUBJECT: {triage.subject}\n"
            f"CUSTOMER MESSAGE: {triage.ticket_body_redacted}\n\n"
            f"RETRIEVED CONTEXT & TOOLS:\n{tool_summary}\n\n"
            "INSTRUCTIONS: Write a concise, actionable support response with [KB-xxx] citations."
        )

        chat_res = self.provider.chat(
            system_prompt=sys_prompt,
            user_prompt=usr_prompt,
            stage="drafter",
            agent_name="agent-drafter",
            correlation_id=ticket_id,
            consumer=self.consumer,
        )
        safe_reply = _ensure_grounded_citation_and_pii_safety(chat_res.text, citations)

        draft_evt = SupportAgentDraftEvent(
            ticket_id=ticket_id,
            customer_id=triage.customer_id,
            subject=triage.subject,
            ticket_body_redacted=triage.ticket_body_redacted,
            revision=1,
            category=triage.category,
            urgency=triage.urgency,
            reply_body=safe_reply,
            citations=citations,
            tool_context_summary=tool_summary[:2500],
            model_version=chat_res.model,
        )
        self.producer.produce_event(
            TOPIC_AGENT_DRAFTS,
            key=ticket_id,
            payload=draft_evt,
            flush=True,
        )
        ctx.initial_draft_emitted = True
        ctx.highest_revision_emitted = 1
        AGENT_EVENTS_TOTAL.labels(agent="drafter", outcome="draft_v1", consumer_mode="classic").inc()
        logger.info("Emitted initial draft %s (rev=1) for ticket=%s", draft_evt.draft_id, ticket_id)
        return AckAction.ACCEPT

    def _emit_revised_draft(self, review: SupportAgentReviewEvent) -> AckAction:
        next_rev = review.revision + 1
        ctx = self._state.setdefault(review.ticket_id, _TicketDraftContext())
        if ctx.highest_revision_emitted >= next_rev:
            return AckAction.ACCEPT

        snap = review.draft_snapshot or {}
        citations: list[dict[str, Any]] = list(snap.get("citations") or [])
        tool_summary = str(snap.get("tool_context_summary") or "")
        subject = str(snap.get("subject") or (ctx.triage.subject if ctx.triage else "Support Request"))
        customer_id = str(snap.get("customer_id") or (ctx.triage.customer_id if ctx.triage else "cust"))
        body_redacted = str(
            snap.get("ticket_body_redacted")
            or (ctx.triage.ticket_body_redacted if ctx.triage else "")
        )
        category = str(snap.get("category") or (ctx.triage.category if ctx.triage else "general"))
        urgency = str(snap.get("urgency") or (ctx.triage.urgency if ctx.triage else "p3_normal"))

        sys_prompt = (
            "You are revising a customer support draft based on automated QA reviewer feedback. "
            "Fix every flagged policy issue, cite [KB-xxx] articles, and omit all PII."
        )
        usr_prompt = (
            f"TICKET: {review.ticket_id}\n"
            f"SUBJECT: {subject}\n"
            f"PREVIOUS DRAFT: {snap.get('reply_body', '')}\n"
            f"REVIEWER FEEDBACK ({review.policy_flags}): {review.feedback}\n"
            f"CONTEXT:\n{tool_summary}"
        )
        chat_res = self.provider.chat(
            system_prompt=sys_prompt,
            user_prompt=usr_prompt,
            stage="drafter_revision",
            agent_name="agent-drafter",
            correlation_id=review.ticket_id,
            consumer=self.consumer,
        )
        safe_reply = _ensure_grounded_citation_and_pii_safety(chat_res.text, citations)
        draft_evt = SupportAgentDraftEvent(
            ticket_id=review.ticket_id,
            customer_id=customer_id,
            subject=subject,
            ticket_body_redacted=body_redacted,
            revision=next_rev,
            category=category,
            urgency=urgency,
            reply_body=safe_reply,
            citations=citations,
            tool_context_summary=tool_summary[:2500],
            model_version=chat_res.model,
        )
        self.producer.produce_event(
            TOPIC_AGENT_DRAFTS,
            key=review.ticket_id,
            payload=draft_evt,
            flush=True,
        )
        ctx.highest_revision_emitted = next_rev
        AGENT_EVENTS_TOTAL.labels(agent="drafter", outcome="draft_revised", consumer_mode="classic").inc()
        logger.info("Emitted revised draft %s (rev=%d) for ticket=%s", draft_evt.draft_id, next_rev, review.ticket_id)
        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        if self._outage_circuit_open:
            if not self.provider.health_check():
                self.consumer.pause_for(
                    self.settings.provider_outage_probe_seconds, reason="provider_outage"
                )
                time.sleep(min(0.4, self.settings.provider_outage_probe_seconds))
                return 0
            self._outage_circuit_open = False
        return self.consumer.step(self.handle_record, poll_timeout=poll_timeout)

    def run_forever(self) -> None:
        logger.info("Starting DraftingAgent joining triage + tool results -> %s", TOPIC_AGENT_DRAFTS)
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()

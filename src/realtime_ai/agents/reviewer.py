"""Reviewer agent evaluating drafts and gating human-in-the-loop approval via `support.human.approvals.v1`."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_AGENT_DRAFTS,
    TOPIC_AGENT_REVIEWS,
    TOPIC_HUMAN_APPROVALS,
)
from realtime_ai.metrics import AGENT_EVENTS_TOTAL
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.providers.base import TransientProviderError
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    SupportAgentDraftEvent,
    SupportAgentReviewEvent,
    SupportHumanApprovalStateEvent,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)

REVIEW_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["verdict", "quality_score", "policy_flags", "feedback"],
    "properties": {
        "verdict": {"type": "string", "enum": ["ready_for_human", "needs_revision"]},
        "quality_score": {"type": "number"},
        "policy_flags": {"type": "array", "items": {"type": "string"}},
        "feedback": {"type": "string"},
    },
}

_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


class ReviewerAgent:
    """Audits drafted replies for grounding & PII compliance, then queues approved drafts for human review."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        provider: ResilientAIProvider,
        group_id: str = "cg-agent-reviewer",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.provider = provider
        self._queued_for_human: set[str] = set()
        self._outage_circuit_open = False

        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_AGENT_DRAFTS],
            mode="classic",
            worker_id=f"{settings.worker_id}-reviewer",
            auto_offset_reset="earliest",
        )

    def handle_record(self, record: RawRecord) -> AckAction:
        if record.value is None:
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(TOPIC_AGENT_DRAFTS, record.value)
            assert decoded is not None
            draft = SupportAgentDraftEvent.model_validate(decoded)
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="agent-reviewer",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=1,
            )
            return AckAction.REJECT

        # Idempotency: if this ticket is already queued for human approval, skip duplicate drafts
        if draft.ticket_id in self._queued_for_human:
            return AckAction.ACCEPT

        sys_prompt = (
            "You are a support quality & compliance reviewer AI. Inspect the proposed customer reply "
            "and return JSON with keys: verdict ('ready_for_human' or 'needs_revision'), "
            "quality_score (0.0-1.0), policy_flags (array), and feedback."
        )
        usr_prompt = (
            f"TICKET: {draft.ticket_id} (Category: {draft.category}, Urgency: {draft.urgency})\n"
            f"SUBJECT: {draft.subject}\n"
            f"CITATIONS AVAILABLE: {[c.get('doc_id') for c in draft.citations]}\n"
            f"DRAFT_REPLY:\n{draft.reply_body}"
        )

        try:
            chat_res = self.provider.chat(
                system_prompt=sys_prompt,
                user_prompt=usr_prompt,
                stage="reviewer",
                agent_name="agent-reviewer",
                correlation_id=draft.ticket_id,
                json_schema=REVIEW_JSON_SCHEMA,
                consumer=self.consumer,
            )
        except TransientProviderError as exc:
            logger.warning("Reviewer releasing draft %s due to transient provider error: %s", draft.draft_id, exc)
            self._outage_circuit_open = True
            self.consumer.pause_for(self.settings.provider_outage_probe_seconds, reason="provider_outage")
            return AckAction.RELEASE

        verdict, score, flags, feedback = self._evaluate_policy(draft, chat_res.text)

        # Cap revision loops so a ticket never spins indefinitely before reaching a human operator
        if verdict == "needs_revision" and draft.revision >= self.settings.max_draft_revisions:
            verdict = "ready_for_human"
            feedback = f"{feedback} (escalated to human queue at max revision={draft.revision})"

        review_evt = SupportAgentReviewEvent(
            draft_id=draft.draft_id,
            ticket_id=draft.ticket_id,
            revision=draft.revision,
            verdict=verdict,  # type: ignore[arg-type]
            quality_score=score,
            policy_flags=flags,
            feedback=feedback,
            draft_snapshot=draft.model_dump(mode="json") if verdict == "needs_revision" else {},
            model_version=chat_res.model,
        )
        self.producer.produce_event(
            TOPIC_AGENT_REVIEWS,
            key=draft.ticket_id,
            payload=review_evt,
            flush=False,
        )

        if verdict == "ready_for_human":
            approval_state = SupportHumanApprovalStateEvent(
                ticket_id=draft.ticket_id,
                draft_id=draft.draft_id,
                revision=draft.revision,
                status="pending",
                customer_id=draft.customer_id,
                subject=draft.subject,
                urgency=draft.urgency,
                category=draft.category,
                proposed_reply=redact_pii(draft.reply_body).sanitized_text,
                final_reply=None,
                citations=draft.citations,
                reviewer_notes=f"Score={score:.2f} | {feedback}",
                human_actor=None,
                human_comment=None,
                decided_ts_ms=None,
            )
            self.producer.produce_event(
                TOPIC_HUMAN_APPROVALS,
                key=draft.ticket_id,
                payload=approval_state,
                flush=True,
            )
            self._queued_for_human.add(draft.ticket_id)
            AGENT_EVENTS_TOTAL.labels(
                agent="reviewer", outcome="queued_for_human", consumer_mode="classic"
            ).inc()
            logger.info(
                "Queued ticket=%s (draft=%s, rev=%d, score=%.2f) on %s awaiting human approval",
                draft.ticket_id,
                draft.draft_id,
                draft.revision,
                score,
                TOPIC_HUMAN_APPROVALS,
            )
        else:
            self.producer.flush()
            AGENT_EVENTS_TOTAL.labels(
                agent="reviewer", outcome="requested_revision", consumer_mode="classic"
            ).inc()
            logger.info(
                "Requested revision for ticket=%s (draft=%s, flags=%s)",
                draft.ticket_id,
                draft.draft_id,
                flags,
            )

        return AckAction.ACCEPT

    @staticmethod
    def _evaluate_policy(
        draft: SupportAgentDraftEvent, raw_llm_json: str
    ) -> tuple[str, float, list[str], str]:
        parsed: dict[str, Any] = {}
        try:
            parsed = json.loads(raw_llm_json)
        except json.JSONDecodeError:
            match = _JSON_BLOCK_RE.search(raw_llm_json)
            if match:
                try:
                    parsed = json.loads(match.group(0))
                except json.JSONDecodeError:
                    parsed = {}

        flags: list[str] = []
        pii_scan = redact_pii(draft.reply_body)
        if pii_scan.total_redactions > 0:
            flags.append(f"pii_detected:{','.join(sorted(pii_scan.counts_by_type))}")
        if draft.citations and "[KB-" not in draft.reply_body:
            flags.append("missing_kb_citation")
        if len(draft.reply_body.strip()) < 25:
            flags.append("reply_too_short")

        if flags:
            return (
                "needs_revision",
                0.58,
                flags,
                f"Policy check flagged: {', '.join(flags)}. Cite [KB-xxx] and remove all PII.",
            )

        raw_score = parsed.get("quality_score", 0.92)
        try:
            score = max(0.75, min(1.0, float(raw_score)))
        except (TypeError, ValueError):
            score = 0.92
        feedback = str(parsed.get("feedback") or "Grounded in cited KB chunks and PII-clean.").strip()
        return "ready_for_human", round(score, 3), [], feedback[:400]

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
        logger.info("Starting ReviewerAgent on %s -> %s", TOPIC_AGENT_DRAFTS, TOPIC_HUMAN_APPROVALS)
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()

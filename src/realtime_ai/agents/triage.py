"""Triage agent consuming `support.tickets.created.v1` via KIP-932 Share Groups (`workers > partitions`)."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections.abc import Callable
from typing import Any

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_AGENT_TOOL_CALLS,
    TOPIC_AGENT_TRIAGE,
    TOPIC_TICKETS_CREATED,
)
from realtime_ai.metrics import AGENT_EVENTS_TOTAL, PII_REDACTIONS_TOTAL
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.providers.base import ProviderPermanentError, TransientProviderError
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    SupportAgentToolCallEvent,
    SupportAgentTriageEvent,
    SupportTicketCreatedEvent,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)

_VALID_CATEGORIES = {
    "auth_sso",
    "billing_refund",
    "api_webhooks",
    "rate_limits",
    "security_privacy",
    "general",
}
_VALID_URGENCIES = {"p1_critical", "p2_high", "p3_normal", "p4_low"}
_VALID_SENTIMENTS = {"frustrated", "urgent", "neutral", "positive"}
_VALID_TOOLS = {"kb_search", "account_sla_lookup", "billing_ledger_check"}

TRIAGE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "category",
        "urgency",
        "sentiment",
        "summary",
        "search_query",
        "requested_tools",
    ],
    "properties": {
        "category": {"type": "string", "enum": sorted(_VALID_CATEGORIES)},
        "urgency": {"type": "string", "enum": sorted(_VALID_URGENCIES)},
        "sentiment": {"type": "string", "enum": sorted(_VALID_SENTIMENTS)},
        "summary": {"type": "string"},
        "search_query": {"type": "string"},
        "requested_tools": {"type": "array", "items": {"type": "string"}},
    },
}

_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _parse_triage_output(raw_text: str, subject: str, redacted_body: str) -> dict[str, Any]:
    """Leniently parses and normalizes structured LLM JSON so small local models stay rock-solid."""
    parsed: dict[str, Any] = {}
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        match = _JSON_BLOCK_RE.search(raw_text)
        if match:
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                parsed = {}

    combined = f"{subject} {redacted_body}".lower()
    cat = str(parsed.get("category", "")).strip().lower()
    if cat not in _VALID_CATEGORIES:
        if any(k in combined for k in ("compromised", "leaked", "gdpr", "erasure", "security")):
            cat = "security_privacy"
        elif any(k in combined for k in ("saml", "sso", "okta", "scim", "login")):
            cat = "auth_sso"
        elif any(k in combined for k in ("vat", "invoice", "refund", "billing")):
            cat = "billing_refund"
        elif any(k in combined for k in ("429", "rate limit", "throttle", "quota")):
            cat = "rate_limits"
        elif any(k in combined for k in ("webhook", "hmac", "signature")):
            cat = "api_webhooks"
        else:
            cat = "general"

    urg = str(parsed.get("urgency", "")).strip().lower()
    if urg not in _VALID_URGENCIES:
        urg = "p1_critical" if cat == "security_privacy" else ("p2_high" if cat in {"auth_sso", "rate_limits", "api_webhooks"} else "p3_normal")

    sent = str(parsed.get("sentiment", "")).strip().lower()
    if sent not in _VALID_SENTIMENTS:
        sent = "urgent" if urg == "p1_critical" else "neutral"

    raw_tools = parsed.get("requested_tools")
    tools = [str(t).strip() for t in raw_tools] if isinstance(raw_tools, list) else []
    tools = [t for t in tools if t in _VALID_TOOLS]
    if "kb_search" not in tools:
        tools.insert(0, "kb_search")
    if cat == "billing_refund" and "billing_ledger_check" not in tools:
        tools.append("billing_ledger_check")
    elif cat in {"auth_sso", "security_privacy", "rate_limits"} and "account_sla_lookup" not in tools:
        tools.append("account_sla_lookup")
    # Preserve unique deterministic order
    tools = list(dict.fromkeys(tools))

    summary = str(parsed.get("summary") or f"{cat} ticket regarding {subject[:90]}").strip()
    search_query = str(parsed.get("search_query") or f"{subject} {redacted_body[:140]}").strip()

    return {
        "category": cat,
        "urgency": urg,
        "sentiment": sent,
        "summary": summary[:300],
        "search_query": search_query[:240],
        "requested_tools": tools,
    }


class TriageAgent:
    """Classifies incoming tickets and emits triage + tool-call events without calling other agents directly."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        provider: ResilientAIProvider,
        worker_id: str | None = None,
        mid_call_hook: Callable[[SupportTicketCreatedEvent, RawRecord], None] | None = None,
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.provider = provider
        self.worker_id = worker_id or settings.worker_id
        self.mid_call_hook = mid_call_hook
        self._outage_circuit_open = False
        self._extra_call_delay_s = float(os.getenv("TRIAGE_SIMULATED_CALL_SECONDS", "0.0"))

        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.share_group_triage,
            topics=[TOPIC_TICKETS_CREATED],
            mode=settings.kafka_consumer_mode,
            worker_id=self.worker_id,
            auto_offset_reset="earliest",
        )

    def handle_record(self, record: RawRecord) -> AckAction:
        if record.value is None:
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(TOPIC_TICKETS_CREATED, record.value)
            assert decoded is not None
            ticket = SupportTicketCreatedEvent.model_validate(decoded)
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage=f"triage:{self.worker_id}",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=record.delivery_count,
            )
            AGENT_EVENTS_TOTAL.labels(
                agent="triage", outcome="dlq", consumer_mode=self.settings.kafka_consumer_mode
            ).inc()
            return AckAction.REJECT

        subj_red = redact_pii(ticket.subject)
        body_red = redact_pii(ticket.body)
        for ptype, count in {**subj_red.counts_by_type, **body_red.counts_by_type}.items():
            PII_REDACTIONS_TOTAL.labels(pii_type=ptype).inc(count)

        if self.mid_call_hook is not None:
            self.mid_call_hook(ticket, record)
        slow_marker_s = 10.0 if "[SIMULATE_SLOW_CALL_8S]" in ticket.body else self._extra_call_delay_s
        if slow_marker_s > 0:
            logger.info(
                "MID_CALL_IN_PROGRESS worker=%s ticket=%s delivery_count=%d sleep=%.1fs",
                self.worker_id,
                ticket.ticket_id,
                record.delivery_count,
                slow_marker_s,
            )
            time.sleep(slow_marker_s)

        sys_prompt = (
            "You are a senior B2B SaaS support triage AI. Classify the support ticket into JSON "
            "with keys: category, urgency, sentiment, summary, search_query, requested_tools."
        )
        usr_prompt = (
            f"Ticket ID: {ticket.ticket_id}\n"
            f"Account Tier: {ticket.account_tier}\n"
            f"Subject: {subj_red.sanitized_text}\n"
            f"Body: {body_red.sanitized_text}"
        )

        try:
            chat_res = self.provider.chat(
                system_prompt=sys_prompt,
                user_prompt=usr_prompt,
                stage="triage",
                agent_name=self.worker_id,
                correlation_id=ticket.ticket_id,
                json_schema=TRIAGE_JSON_SCHEMA,
                consumer=self.consumer,
            )
        except TransientProviderError as exc:
            logger.warning(
                "Triage worker %s releasing ticket %s due to provider outage/timeout (%s); opening health circuit",
                self.worker_id,
                ticket.ticket_id,
                exc,
            )
            self._outage_circuit_open = True
            self.consumer.pause_for(
                self.settings.provider_outage_probe_seconds, reason="provider_outage"
            )
            AGENT_EVENTS_TOTAL.labels(
                agent="triage", outcome="released_retry", consumer_mode=self.settings.kafka_consumer_mode
            ).inc()
            return AckAction.RELEASE
        except (ProviderPermanentError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=ticket.ticket_id,
                raw_payload=record.value,
                worker_stage=f"triage:{self.worker_id}",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=record.delivery_count,
            )
            AGENT_EVENTS_TOTAL.labels(
                agent="triage", outcome="dlq", consumer_mode=self.settings.kafka_consumer_mode
            ).inc()
            return AckAction.REJECT

        normalized = _parse_triage_output(
            chat_res.text, subj_red.sanitized_text, body_red.sanitized_text
        )
        tools: list[str] = normalized["requested_tools"]

        triage_evt = SupportAgentTriageEvent(
            ticket_id=ticket.ticket_id,
            customer_id=ticket.customer_id,
            account_tier=ticket.account_tier,
            subject=subj_red.sanitized_text,
            ticket_body_redacted=body_red.sanitized_text,
            category=normalized["category"],
            urgency=normalized["urgency"],
            sentiment=normalized["sentiment"],
            summary=normalized["summary"],
            search_query=normalized["search_query"],
            requested_tools=tools,
            worker_id=self.worker_id,
            delivery_count=record.delivery_count,
            model_version=chat_res.model,
        )
        self.producer.produce_event(
            TOPIC_AGENT_TRIAGE,
            key=ticket.ticket_id,
            payload=triage_evt,
            flush=False,
        )

        total_calls = len(tools)
        for tool_name in tools:
            args: dict[str, Any] = {
                "customer_id": ticket.customer_id,
                "account_tier": ticket.account_tier,
                "query": normalized["search_query"],
                "category": normalized["category"],
            }
            call_evt = SupportAgentToolCallEvent(
                ticket_id=ticket.ticket_id,
                requested_by_agent=self.worker_id,
                tool_name=tool_name,  # type: ignore[arg-type]
                arguments=args,
                total_calls_for_ticket=total_calls,
            )
            self.producer.produce_event(
                TOPIC_AGENT_TOOL_CALLS,
                key=ticket.ticket_id,
                payload=call_evt,
                flush=False,
            )

        self.producer.flush()
        AGENT_EVENTS_TOTAL.labels(
            agent="triage", outcome="accepted", consumer_mode=self.settings.kafka_consumer_mode
        ).inc()
        logger.info(
            "Triaged ticket=%s via %s (%s mode, delivery=%d, category=%s, urgency=%s, tools=%s)",
            ticket.ticket_id,
            self.worker_id,
            self.settings.kafka_consumer_mode,
            record.delivery_count,
            triage_evt.category,
            triage_evt.urgency,
            tools,
        )
        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        if self._outage_circuit_open:
            if not self.provider.health_check():
                self.consumer.pause_for(
                    self.settings.provider_outage_probe_seconds, reason="provider_outage"
                )
                time.sleep(min(0.4, self.settings.provider_outage_probe_seconds))
                return 0
            logger.info("AI provider health restored; closing outage circuit on %s", self.worker_id)
            self._outage_circuit_open = False
        return self.consumer.step(self.handle_record, poll_timeout=poll_timeout)

    def run_forever(self) -> None:
        logger.info(
            "Starting TriageAgent worker=%s on %s (mode=%s, group=%s)",
            self.worker_id,
            TOPIC_TICKETS_CREATED,
            self.settings.kafka_consumer_mode,
            self.settings.share_group_triage,
        )
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()

"""Event-driven tool execution worker consuming `support.agent.tool.calls.v1` and emitting results."""

from __future__ import annotations

import logging
import time

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_AGENT_TOOL_CALLS,
    TOPIC_AGENT_TOOL_RESULTS,
    TOPIC_KB_INDEX_CONTROL,
)
from realtime_ai.metrics import AGENT_EVENTS_TOTAL
from realtime_ai.pipeline.vector_store import ActiveIndexSnapshot, QdrantVectorRepository
from realtime_ai.providers.base import TransientProviderError
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    KbIndexControlEvent,
    SupportAgentToolCallEvent,
    SupportAgentToolResultEvent,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec, SchemaWireError

logger = logging.getLogger(__name__)

_TIER_SLAS = {
    "enterprise": {
        "p1_response_minutes": 15,
        "p2_response_minutes": 60,
        "dedicated_tam": True,
        "rps_ceiling": "600 RPS sustained / 1,500 burst",
    },
    "pro": {
        "p1_response_minutes": 60,
        "p2_response_minutes": 240,
        "dedicated_tam": False,
        "rps_ceiling": "120 RPS sustained / 250 burst",
    },
    "free": {
        "p1_response_minutes": 1440,
        "p2_response_minutes": 2880,
        "dedicated_tam": False,
        "rps_ceiling": "10 RPS sustained / 20 burst",
    },
}


class ToolExecutorAgent:
    """Executes agent tool calls asynchronously via Kafka topics (`kb_search`, `account_sla_lookup`, `billing_ledger_check`)."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        producer: EventProducer,
        provider: ResilientAIProvider,
        vector_repo: QdrantVectorRepository,
        group_id: str = "cg-agent-tools",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.producer = producer
        self.provider = provider
        self.vector_repo = vector_repo
        self._outage_circuit_open = False

        self._ctrl_consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=f"cg-tools-ctrl-{settings.worker_id}",
            topics=[TOPIC_KB_INDEX_CONTROL],
            mode="classic",
            worker_id=f"{settings.worker_id}-tools-ctrl",
            auto_offset_reset="earliest",
        )
        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_AGENT_TOOL_CALLS],
            mode="classic",
            worker_id=f"{settings.worker_id}-tools",
            auto_offset_reset="earliest",
        )

    def _sync_control(self) -> None:
        def _on_ctrl(rec: RawRecord) -> AckAction:
            if rec.value is not None:
                decoded = self.codec.decode(TOPIC_KB_INDEX_CONTROL, rec.value)
                if decoded:
                    evt = KbIndexControlEvent.model_validate(decoded)
                    self.vector_repo.apply_control_snapshot(
                        ActiveIndexSnapshot(
                            alias_name=evt.alias_name,
                            collection_name=evt.collection_name,
                            embedding_model=evt.embedding_model,
                            embedding_version=evt.embedding_version,
                            dimensions=evt.dimensions,
                        )
                    )
            return AckAction.ACCEPT

        while self._ctrl_consumer.step(_on_ctrl, poll_timeout=0.02) > 0:
            pass

    def handle_record(self, record: RawRecord) -> AckAction:
        if record.value is None:
            return AckAction.ACCEPT

        try:
            decoded = self.codec.decode(TOPIC_AGENT_TOOL_CALLS, record.value)
            assert decoded is not None
            call = SupportAgentToolCallEvent.model_validate(decoded)
        except (SchemaWireError, Exception) as exc:
            self.producer.send_to_dlq(
                source_topic=record.topic,
                source_partition=record.partition,
                source_offset=record.offset,
                message_key=record.key,
                raw_payload=record.value,
                worker_stage="agent-tools",
                error_type=type(exc).__name__,
                error_message=str(exc),
                retry_attempts=1,
            )
            return AckAction.REJECT

        t0 = time.perf_counter()
        citations_out: list[dict[str, object]] = []
        status = "ok"

        try:
            if call.tool_name == "kb_search":
                query = str(call.arguments.get("query") or "")
                snap = self.vector_repo.resolve_active_snapshot(
                    default_model=self.settings.embedding_model,
                    default_version=self.settings.embedding_version,
                    default_dimensions=self.settings.embedding_dimensions,
                )
                emb = self.provider.embed(
                    [query],
                    model=snap.embedding_model,
                    stage="tool_kb_search",
                    agent_name="agent-tools",
                    correlation_id=call.ticket_id,
                    consumer=self.consumer,
                )
                hits = self.vector_repo.search(
                    collection_name=snap.collection_name,
                    query_vector=emb.vectors[0],
                    top_k=3,
                )
                citations_out = [h.model_dump(mode="json") for h in hits]
                if hits:
                    formatted = "\n".join(
                        f"- [{h.doc_id}] {h.title} (score={h.score}, ver={h.embedding_version}): {h.snippet}"
                        for h in hits
                    )
                    output = f"Retrieved {len(hits)} KB chunks from {snap.collection_name}:\n{formatted}"
                else:
                    output = "No matching KB articles found."

            elif call.tool_name == "account_sla_lookup":
                tier = str(call.arguments.get("account_tier") or "pro").lower()
                customer_id = str(call.arguments.get("customer_id") or "unknown")
                sla = _TIER_SLAS.get(tier, _TIER_SLAS["pro"])
                output = (
                    f"Account {customer_id} (tier={tier}): "
                    f"P1 SLA={sla['p1_response_minutes']}m, P2 SLA={sla['p2_response_minutes']}m, "
                    f"Quota={sla['rps_ceiling']}, Dedicated TAM={sla['dedicated_tam']}."
                )

            elif call.tool_name == "billing_ledger_check":
                customer_id = str(call.arguments.get("customer_id") or "unknown")
                output = (
                    f"Billing ledger for {customer_id}: Latest invoice INV-2026-8841 issued 6 days ago "
                    f"(eligible for 30-day EU VAT Reverse-Charge Credit Note and prorated seat credit)."
                )
            else:
                status = "error"
                output = f"Unsupported tool: {call.tool_name}"

        except TransientProviderError as exc:
            logger.warning("Transient error during tool %s for %s: %s", call.tool_name, call.ticket_id, exc)
            self._outage_circuit_open = True
            self.consumer.pause_for(self.settings.provider_outage_probe_seconds, reason="provider_outage")
            return AckAction.RELEASE

        latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        res_evt = SupportAgentToolResultEvent(
            call_id=call.call_id,
            ticket_id=call.ticket_id,
            tool_name=call.tool_name,
            status=status,  # type: ignore[arg-type]
            output=output,
            citations=citations_out,
            total_calls_for_ticket=call.total_calls_for_ticket,
            latency_ms=latency_ms,
        )
        self.producer.produce_event(
            TOPIC_AGENT_TOOL_RESULTS,
            key=call.ticket_id,
            payload=res_evt,
            flush=True,
        )
        AGENT_EVENTS_TOTAL.labels(agent="tools", outcome="accepted", consumer_mode="classic").inc()
        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        self._sync_control()
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
        logger.info("Starting ToolExecutorAgent on %s -> %s", TOPIC_AGENT_TOOL_CALLS, TOPIC_AGENT_TOOL_RESULTS)
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()
        self._ctrl_consumer.close()

"""Compliance audit stream consumer building real-time Prometheus token & USD cost metrics from `ai.audit.events.v1`."""

from __future__ import annotations

import logging
import time
from collections import deque

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.topics import TOPIC_AI_AUDIT
from realtime_ai.metrics import (
    AUDIT_ESTIMATED_COST_USD_TOTAL,
    AUDIT_RECORDS_CONSUMED_TOTAL,
    AUDIT_STAGE_LATENCY_MS,
    AUDIT_TOKENS_TOTAL,
)
from realtime_ai.schemas.models import AiAuditEvent
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


class AuditCostExporter:
    """Consumes `ai.audit.events.v1` (7-year compliance topic) to materialize cost & token governance metrics."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        group_id: str = "cg-audit-cost-exporter",
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.recent_events: deque[AiAuditEvent] = deque(maxlen=250)
        self.total_prompt_tokens: int = 0
        self.total_completion_tokens: int = 0
        self.total_cost_usd: float = 0.0

        self.consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            topics=[TOPIC_AI_AUDIT],
            mode="classic",
            worker_id=f"{settings.worker_id}-audit",
            auto_offset_reset="earliest",
        )

    def handle_record(self, record: RawRecord) -> AckAction:
        if record.value is None:
            return AckAction.ACCEPT
        decoded = self.codec.decode(TOPIC_AI_AUDIT, record.value)
        if not decoded:
            return AckAction.ACCEPT

        evt = AiAuditEvent.model_validate(decoded)
        self.recent_events.append(evt)
        self.total_prompt_tokens += evt.prompt_tokens
        self.total_completion_tokens += evt.completion_tokens
        self.total_cost_usd = round(self.total_cost_usd + evt.estimated_cost_usd, 8)

        AUDIT_RECORDS_CONSUMED_TOTAL.labels(
            provider=evt.provider,
            model_version=evt.model_version,
            stage=evt.stage,
            operation=evt.operation,
            status=evt.status,
        ).inc()
        if evt.prompt_tokens > 0:
            AUDIT_TOKENS_TOTAL.labels(
                provider=evt.provider,
                model_version=evt.model_version,
                stage=evt.stage,
                token_type="prompt",
            ).inc(evt.prompt_tokens)
        if evt.completion_tokens > 0:
            AUDIT_TOKENS_TOTAL.labels(
                provider=evt.provider,
                model_version=evt.model_version,
                stage=evt.stage,
                token_type="completion",
            ).inc(evt.completion_tokens)
        if evt.estimated_cost_usd > 0:
            AUDIT_ESTIMATED_COST_USD_TOTAL.labels(
                provider=evt.provider,
                model_version=evt.model_version,
                stage=evt.stage,
            ).inc(evt.estimated_cost_usd)
        AUDIT_STAGE_LATENCY_MS.labels(
            stage=evt.stage,
            model_version=evt.model_version,
        ).observe(evt.latency_ms)

        return AckAction.ACCEPT

    def step(self, poll_timeout: float = 0.5) -> int:
        return self.consumer.step(self.handle_record, poll_timeout=poll_timeout)

    def run_forever(self) -> None:
        logger.info("Starting AuditCostExporter reading compliance topic %s", TOPIC_AI_AUDIT)
        try:
            while True:
                if self.step(poll_timeout=0.5) == 0:
                    time.sleep(0.05)
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()

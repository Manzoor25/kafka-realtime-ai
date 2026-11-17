"""Kafka log-backed state materializer for freshness telemetry, index cutovers, and human approvals."""

from __future__ import annotations

import logging
import statistics
import threading
import time
import uuid
from collections import deque
from typing import Any

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import AckAction, RawRecord, WorkerConsumer
from realtime_ai.kafka.topics import (
    TOPIC_AI_AUDIT,
    TOPIC_HUMAN_APPROVALS,
    TOPIC_KB_INDEX_CONTROL,
    TOPIC_KB_INDEX_TELEMETRY,
    TOPIC_REPLIES_APPROVED,
)
from realtime_ai.metrics import HUMAN_APPROVALS_GAUGE
from realtime_ai.pipeline.vector_store import ActiveIndexSnapshot, QdrantVectorRepository
from realtime_ai.schemas.models import (
    AiAuditEvent,
    KbIndexControlEvent,
    KbIndexTelemetryEvent,
    SupportHumanApprovalStateEvent,
    SupportReplyApprovedEvent,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return round(sorted_vals[0], 2)
    rank = (len(sorted_vals) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return round(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac, 2)


class ApiStreamMaterializer:
    """Rebuilds and continuously updates API read views directly from Kafka compacted & event topics."""

    def __init__(
        self,
        *,
        settings: Settings,
        codec: SchemaRegistryCodec,
        vector_repo: QdrantVectorRepository,
    ) -> None:
        self.settings = settings
        self.codec = codec
        self.vector_repo = vector_repo
        self._lock = threading.RLock()

        self._approvals: dict[str, SupportHumanApprovalStateEvent] = {}
        self._approved_replies: list[SupportReplyApprovedEvent] = []
        self._telemetry_events: deque[KbIndexTelemetryEvent] = deque(maxlen=500)
        self._recent_audit: deque[AiAuditEvent] = deque(maxlen=100)

        instance_suffix = uuid.uuid4().hex[:8]
        self._consumer = WorkerConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=f"cg-api-materializer-{instance_suffix}",
            topics=[
                TOPIC_KB_INDEX_CONTROL,
                TOPIC_KB_INDEX_TELEMETRY,
                TOPIC_HUMAN_APPROVALS,
                TOPIC_REPLIES_APPROVED,
                TOPIC_AI_AUDIT,
            ],
            mode="classic",
            worker_id=f"api-mat-{instance_suffix}",
            auto_offset_reset="earliest",
        )
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def _handle_record(self, rec: RawRecord) -> AckAction:
        if rec.value is None:
            if rec.topic == TOPIC_HUMAN_APPROVALS and rec.key:
                with self._lock:
                    self._approvals.pop(rec.key, None)
                    self._refresh_approval_gauges()
            return AckAction.ACCEPT

        decoded = self.codec.decode(rec.topic, rec.value)
        if not decoded:
            return AckAction.ACCEPT

        if rec.topic == TOPIC_KB_INDEX_CONTROL:
            ctrl = KbIndexControlEvent.model_validate(decoded)
            self.vector_repo.apply_control_snapshot(
                ActiveIndexSnapshot(
                    alias_name=ctrl.alias_name,
                    collection_name=ctrl.collection_name,
                    embedding_model=ctrl.embedding_model,
                    embedding_version=ctrl.embedding_version,
                    dimensions=ctrl.dimensions,
                )
            )
        elif rec.topic == TOPIC_KB_INDEX_TELEMETRY:
            tel = KbIndexTelemetryEvent.model_validate(decoded)
            with self._lock:
                self._telemetry_events.append(tel)
        elif rec.topic == TOPIC_HUMAN_APPROVALS:
            app_evt = SupportHumanApprovalStateEvent.model_validate(decoded)
            with self._lock:
                self._approvals[app_evt.ticket_id] = app_evt
                self._refresh_approval_gauges()
        elif rec.topic == TOPIC_REPLIES_APPROVED:
            rep = SupportReplyApprovedEvent.model_validate(decoded)
            with self._lock:
                if not any(r.reply_id == rep.reply_id for r in self._approved_replies):
                    self._approved_replies.append(rep)
        elif rec.topic == TOPIC_AI_AUDIT:
            aud = AiAuditEvent.model_validate(decoded)
            with self._lock:
                self._recent_audit.append(aud)

        return AckAction.ACCEPT

    def _refresh_approval_gauges(self) -> None:
        counts = {"pending": 0, "approved": 0, "rejected": 0}
        for item in self._approvals.values():
            counts[item.status] = counts.get(item.status, 0) + 1
        for status, count in counts.items():
            HUMAN_APPROVALS_GAUGE.labels(status=status).set(count)

    def sync_once(self, max_batches: int = 30, poll_timeout: float = 0.05) -> int:
        total = 0
        for _ in range(max_batches):
            n = self._consumer.step(self._handle_record, poll_timeout=poll_timeout)
            total += n
            if n == 0:
                break
        return total

    def start_background(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()

        def _loop() -> None:
            while not self._stop_event.is_set():
                try:
                    n = self._consumer.step(self._handle_record, poll_timeout=0.25)
                    if n == 0:
                        time.sleep(0.05)
                except Exception as exc:
                    logger.warning("API stream materializer poll warning: %s", exc)
                    time.sleep(0.5)

        self._thread = threading.Thread(target=_loop, name="api-materializer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
        self._consumer.close()

    def record_local_approval_update(
        self,
        state_evt: SupportHumanApprovalStateEvent,
        reply_evt: SupportReplyApprovedEvent | None = None,
    ) -> None:
        with self._lock:
            self._approvals[state_evt.ticket_id] = state_evt
            if reply_evt is not None and not any(
                r.ticket_id == reply_evt.ticket_id for r in self._approved_replies
            ):
                self._approved_replies.append(reply_evt)
            self._refresh_approval_gauges()

    def get_freshness_summary(self) -> dict[str, Any]:
        with self._lock:
            samples = list(self._telemetry_events)
        if not samples:
            return {
                "samples_observed": 0,
                "last_freshness_ms": None,
                "p50_freshness_ms": None,
                "p95_freshness_ms": None,
                "mean_freshness_ms": None,
                "min_freshness_ms": None,
                "max_freshness_ms": None,
                "last_doc_id": None,
                "last_op": None,
                "last_indexed_ts_ms": None,
            }
        vals = sorted(s.freshness_ms for s in samples)
        latest = samples[-1]
        return {
            "samples_observed": len(vals),
            "last_freshness_ms": round(latest.freshness_ms, 2),
            "p50_freshness_ms": _percentile(vals, 50.0),
            "p95_freshness_ms": _percentile(vals, 95.0),
            "mean_freshness_ms": round(statistics.fmean(vals), 2),
            "min_freshness_ms": round(vals[0], 2),
            "max_freshness_ms": round(vals[-1], 2),
            "last_doc_id": latest.doc_id,
            "last_op": latest.op,
            "last_collection": latest.collection_name,
            "last_embedding_version": latest.embedding_version,
            "last_indexed_ts_ms": latest.indexed_ts_ms,
        }

    def list_approvals(self, status: str | None = None) -> list[SupportHumanApprovalStateEvent]:
        with self._lock:
            items = list(self._approvals.values())
        if status:
            items = [i for i in items if i.status == status]
        return sorted(items, key=lambda x: x.queued_ts_ms, reverse=True)

    def get_approval(self, ticket_id: str) -> SupportHumanApprovalStateEvent | None:
        with self._lock:
            return self._approvals.get(ticket_id)

    def list_approved_replies(self, ticket_id: str | None = None) -> list[SupportReplyApprovedEvent]:
        with self._lock:
            items = list(self._approved_replies)
        if ticket_id:
            items = [r for r in items if r.ticket_id == ticket_id]
        return items

    def list_recent_audit(self, limit: int = 40) -> list[AiAuditEvent]:
        with self._lock:
            items = list(self._recent_audit)
        return list(reversed(items))[:limit]

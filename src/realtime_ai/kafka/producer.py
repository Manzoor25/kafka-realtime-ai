"""Idempotent Kafka producer with Schema Registry wire serialization, tombstones, and DLQ routing."""

from __future__ import annotations

import base64
import logging
from typing import Any

from confluent_kafka import Producer
from pydantic import BaseModel

from realtime_ai.kafka.inmemory import get_inmemory_cluster
from realtime_ai.kafka.topics import TOPIC_DLQ
from realtime_ai.metrics import DLQ_EVENTS_TOTAL
from realtime_ai.schemas.models import DeadLetterEvent
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


class EventProducer:
    """Produces schema-validated events and tombstones to Apache Kafka or the in-memory test bus."""

    def __init__(self, bootstrap_servers: str, codec: SchemaRegistryCodec, client_id: str = "krai-producer") -> None:
        self._bootstrap_servers = bootstrap_servers
        self._codec = codec
        self._in_memory = bootstrap_servers.startswith("memory://")
        if self._in_memory:
            self._cluster = get_inmemory_cluster(bootstrap_servers)
            self._producer = None
        else:
            self._cluster = None
            self._producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": client_id,
                    "acks": "all",
                    "enable.idempotence": True,
                    "retries": 10,
                    "linger.ms": 5,
                }
            )

    def produce_event(
        self,
        topic: str,
        key: str | None,
        payload: BaseModel | dict[str, Any],
        headers: dict[str, str] | None = None,
        flush: bool = False,
    ) -> None:
        body_dict = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
        wire_bytes = self._codec.encode(topic, body_dict)
        self._send_raw(topic=topic, key=key, value=wire_bytes, headers=headers, flush=flush)

    def produce_tombstone(
        self,
        topic: str,
        key: str,
        headers: dict[str, str] | None = None,
        flush: bool = True,
    ) -> None:
        """Emits a null-payload Kafka tombstone keyed by `key` (e.g., document deletion)."""
        merged = {"event_type": "tombstone", **(headers or {})}
        self._send_raw(topic=topic, key=key, value=None, headers=merged, flush=flush)

    def send_to_dlq(
        self,
        *,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        message_key: str | None,
        raw_payload: bytes | None,
        worker_stage: str,
        error_type: str,
        error_message: str,
        retry_attempts: int,
    ) -> DeadLetterEvent:
        dlq_evt = DeadLetterEvent(
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            message_key=message_key,
            worker_stage=worker_stage,
            error_type=error_type,
            error_message=error_message[:2000],
            retry_attempts=retry_attempts,
            raw_payload_b64=base64.b64encode(raw_payload or b"").decode("ascii"),
        )
        self.produce_event(TOPIC_DLQ, key=message_key or dlq_evt.dlq_id, payload=dlq_evt, flush=True)
        DLQ_EVENTS_TOTAL.labels(
            source_topic=source_topic, worker_stage=worker_stage, error_type=error_type
        ).inc()
        logger.error(
            "Routed failed record %s[%d]@%d (key=%s) from %s to %s: %s",
            source_topic,
            source_partition,
            source_offset,
            message_key,
            worker_stage,
            TOPIC_DLQ,
            error_message,
        )
        return dlq_evt

    def _send_raw(
        self,
        *,
        topic: str,
        key: str | None,
        value: bytes | None,
        headers: dict[str, str] | None,
        flush: bool,
    ) -> None:
        key_bytes = key.encode("utf-8") if key is not None else None
        hdr_list = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

        if self._in_memory:
            assert self._cluster is not None
            self._cluster.produce(topic=topic, key=key_bytes, value=value, headers=hdr_list)
            return

        assert self._producer is not None
        delivery_errors: list[str] = []

        def _on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                delivery_errors.append(str(err))

        self._producer.produce(
            topic=topic,
            key=key_bytes,
            value=value,
            headers=hdr_list or None,
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)
        if flush:
            remaining = self._producer.flush(timeout=15.0)
            if remaining > 0:
                raise TimeoutError(f"Kafka flush timed out with {remaining} undelivered record(s)")
            if delivery_errors:
                raise RuntimeError(f"Kafka delivery failed on {topic}: {delivery_errors[0]}")

    def flush(self, timeout: float = 10.0) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=timeout)

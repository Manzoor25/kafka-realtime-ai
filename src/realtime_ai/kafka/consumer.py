"""Unified Kafka consumer adapters supporting KIP-932 Share Groups and Classic Consumer Groups.

Notes on KIP-932 & KIP-1222 in confluent-kafka==2.16.1:
- `confluent_kafka.ShareConsumer` supports KIP-932 cooperative share-group consumption (`workers > partitions`),
  explicit per-record acknowledgement (`AcknowledgeType.ACCEPT`, `RELEASE`, `REJECT`), and `Message.delivery_count()`.
- Client-side acquisition-lock renewal (`AcknowledgeType.RENEW`, KIP-1222) and `ShareConsumer.pause()` are not yet
  exposed by `librdkafka` / `confluent-kafka==2.16.1`.
  * In `share` mode, acquisition locks are governed by broker-side `share.record.lock.duration.ms` (with forward-compatible
    `RENEW` probing if a newer librdkafka exposes it), and rate-limit / outage backpressure releases uncommitted records
    before gating `poll()` via a pause timer so unacquired records remain safely queued on the broker.
  * In `classic` mode (`confluent_kafka.Consumer`), `pause(assignment)` / `resume(assignment)` are invoked directly on
    librdkafka while a background/interleaved `poll(0)` heartbeat loop keeps group membership alive during slow LLM calls.
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import AcknowledgeType, Consumer, ShareConsumer, TopicPartition

from realtime_ai.kafka.inmemory import get_inmemory_cluster
from realtime_ai.metrics import CONSUMER_PAUSED_GAUGE, SHARE_GROUP_DELIVERY_COUNT

logger = logging.getLogger(__name__)


class AckAction(str, enum.Enum):
    ACCEPT = "accept"
    RELEASE = "release"
    REJECT = "reject"


@dataclass(frozen=True)
class RawRecord:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes | None
    headers: dict[str, str]
    timestamp_ms: int
    delivery_count: int = 1


def _decode_headers(raw_headers: Any) -> dict[str, str]:
    if not raw_headers:
        return {}
    out: dict[str, str] = {}
    for k, v in raw_headers:
        if v is None:
            out[k] = ""
        elif isinstance(v, bytes):
            out[k] = v.decode("utf-8", errors="replace")
        else:
            out[k] = str(v)
    return out


class WorkerConsumer:
    """Abstract worker consumer supporting `share` (KIP-932) and `classic` modes."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        mode: str = "share",
        worker_id: str = "worker-1",
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.mode = mode
        self.worker_id = worker_id
        self.auto_offset_reset = auto_offset_reset
        self._in_memory = bootstrap_servers.startswith("memory://")
        self._paused_until = 0.0
        self._pause_reason = "rate_limit"
        self._classic_paused_tps: list[TopicPartition] = []
        self._consumer_lock = threading.Lock()

        if self._in_memory:
            self._cluster = get_inmemory_cluster(bootstrap_servers)
            self._share_consumer = None
            self._classic_consumer = None
        elif mode == "share":
            self._cluster = None
            self._classic_consumer = None
            self._share_consumer = ShareConsumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": group_id,
                    "client.id": worker_id,
                    "share.acknowledgement.mode": "explicit",
                    "max.poll.records": 1,
                }
            )
            self._share_consumer.subscribe(topics)
        else:
            self._cluster = None
            self._share_consumer = None
            self._classic_consumer = Consumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": group_id,
                    "client.id": worker_id,
                    "enable.auto.commit": False,
                    "auto.offset.reset": auto_offset_reset,
                    "max.poll.interval.ms": 300000,
                    "session.timeout.ms": 45000,
                }
            )
            self._classic_consumer.subscribe(topics)

    @property
    def supports_lock_renewal(self) -> bool:
        if self._in_memory and self.mode == "share":
            return True
        return hasattr(AcknowledgeType, "RENEW")

    def pause_for(self, seconds: float, reason: str = "rate_limit") -> None:
        """Pauses record consumption during 429 rate-limit windows or provider outages."""
        if seconds <= 0:
            return
        self._paused_until = max(self._paused_until, time.monotonic() + seconds)
        self._pause_reason = reason
        CONSUMER_PAUSED_GAUGE.labels(worker_id=self.worker_id, reason=reason).set(1)
        if self._classic_consumer is not None and not self._classic_paused_tps:
            with self._consumer_lock:
                assigned = self._classic_consumer.assignment()
                if assigned:
                    self._classic_consumer.pause(assigned)
                    self._classic_paused_tps = assigned
                    logger.warning(
                        "Paused classic consumer %s on %d partition(s) for %.2fs (%s)",
                        self.worker_id,
                        len(assigned),
                        seconds,
                        reason,
                    )
        else:
            logger.warning(
                "Gated %s consumer %s polling for %.2fs (%s)",
                self.mode,
                self.worker_id,
                seconds,
                reason,
            )

    def _check_resume(self) -> bool:
        """Returns True if currently paused; sends heartbeats in classic mode while paused."""
        now = time.monotonic()
        if now < self._paused_until:
            if self._classic_consumer is not None:
                self._classic_consumer.poll(0.1)
            else:
                time.sleep(min(0.25, self._paused_until - now))
            return True

        if self._paused_until > 0.0:
            if self._classic_consumer is not None and self._classic_paused_tps:
                self._classic_consumer.resume(self._classic_paused_tps)
                logger.info(
                    "Resumed classic consumer %s on %d partition(s)",
                    self.worker_id,
                    len(self._classic_paused_tps),
                )
                self._classic_paused_tps = []
            CONSUMER_PAUSED_GAUGE.labels(worker_id=self.worker_id, reason=self._pause_reason).set(0)
            self._paused_until = 0.0
        return False

    def step(
        self,
        handler: Callable[[RawRecord], AckAction],
        poll_timeout: float = 0.5,
    ) -> int:
        """Polls at most one batch, runs `handler(record) -> AckAction`, and commits acks."""
        if self._check_resume():
            return 0

        if self._in_memory:
            return self._step_inmemory(handler)
        if self.mode == "share":
            return self._step_share(handler, poll_timeout)
        return self._step_classic(handler, poll_timeout)

    def _step_inmemory(self, handler: Callable[[RawRecord], AckAction]) -> int:
        assert self._cluster is not None
        if self.mode == "share":
            batch = self._cluster.poll_share(
                group_id=self.group_id,
                member_id=self.worker_id,
                topics=self.topics,
                max_records=1,
            )
            if not batch:
                return 0
            processed = 0
            for stored, delivery_count in batch:
                SHARE_GROUP_DELIVERY_COUNT.labels(group_id=self.group_id).observe(delivery_count)
                rec = RawRecord(
                    topic=stored.topic,
                    partition=stored.partition,
                    offset=stored.offset,
                    key=stored.key.decode("utf-8") if stored.key is not None else None,
                    value=stored.value,
                    headers=_decode_headers(stored.headers),
                    timestamp_ms=stored.timestamp_ms,
                    delivery_count=delivery_count,
                )
                stop_renew = threading.Event()
                renew_thread = threading.Thread(
                    target=self._inmemory_renew_loop,
                    args=(stored.topic, stored.partition, stored.offset, stop_renew),
                    daemon=True,
                )
                renew_thread.start()
                try:
                    decision = handler(rec)
                finally:
                    stop_renew.set()
                    renew_thread.join(timeout=0.5)
                self._cluster.acknowledge_share(
                    group_id=self.group_id,
                    member_id=self.worker_id,
                    topic=stored.topic,
                    partition=stored.partition,
                    offset=stored.offset,
                    decision=decision.value,
                )
                processed += 1
            return processed

        records = self._cluster.poll_classic(
            group_id=self.group_id,
            topics=self.topics,
            max_records=5,
            auto_offset_reset=self.auto_offset_reset,
        )
        processed = 0
        for idx, stored in enumerate(records):
            rec = RawRecord(
                topic=stored.topic,
                partition=stored.partition,
                offset=stored.offset,
                key=stored.key.decode("utf-8") if stored.key is not None else None,
                value=stored.value,
                headers=_decode_headers(stored.headers),
                timestamp_ms=stored.timestamp_ms,
                delivery_count=1,
            )
            decision = handler(rec)
            if decision == AckAction.RELEASE:
                self._cluster.rewind_classic(
                    self.group_id, stored.topic, stored.partition, stored.offset
                )
                for rem in records[idx + 1 :]:
                    if (rem.topic, rem.partition) != (stored.topic, stored.partition):
                        self._cluster.rewind_classic(
                            self.group_id, rem.topic, rem.partition, rem.offset
                        )
                break
            processed += 1
        return processed

    def _inmemory_renew_loop(
        self, topic: str, partition: int, offset: int, stop_event: threading.Event
    ) -> None:
        assert self._cluster is not None
        interval = max(0.2, self._cluster.lock_duration_seconds * 0.4)
        while not stop_event.wait(interval):
            self._cluster.acknowledge_share(
                group_id=self.group_id,
                member_id=self.worker_id,
                topic=topic,
                partition=partition,
                offset=offset,
                decision="renew",
            )

    def _step_share(self, handler: Callable[[RawRecord], AckAction], poll_timeout: float) -> int:
        assert self._share_consumer is not None
        messages = self._share_consumer.poll(timeout=poll_timeout)
        if messages is None or len(messages) == 0:
            return 0

        msg_list = list(messages)
        processed = 0
        release_remaining = False

        for msg in msg_list:
            if msg.error():
                # Deserialization or broker record error
                logger.warning("ShareConsumer record error on %s: %s", msg.topic(), msg.error())
                continue
            if release_remaining:
                self._share_consumer.acknowledge(msg, AcknowledgeType.RELEASE)
                continue

            dcount = msg.delivery_count() or 1
            SHARE_GROUP_DELIVERY_COUNT.labels(group_id=self.group_id).observe(dcount)
            ts_type, ts_val = msg.timestamp()
            rec = RawRecord(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key().decode("utf-8") if msg.key() is not None else None,
                value=msg.value(),
                headers=_decode_headers(msg.headers()),
                timestamp_ms=ts_val if ts_type > 0 else int(time.time() * 1000),
                delivery_count=dcount,
            )
            try:
                decision = handler(rec)
            except Exception:
                self._share_consumer.acknowledge(msg, AcknowledgeType.RELEASE)
                release_remaining = True
                self._share_consumer.commit_sync(timeout=5.0)
                raise

            ack_type = {
                AckAction.ACCEPT: AcknowledgeType.ACCEPT,
                AckAction.RELEASE: AcknowledgeType.RELEASE,
                AckAction.REJECT: AcknowledgeType.REJECT,
            }[decision]
            self._share_consumer.acknowledge(msg, ack_type)
            processed += 1
            if decision == AckAction.RELEASE:
                release_remaining = True

        commit_res = self._share_consumer.commit_sync(timeout=10.0)
        for tp, exc in commit_res.items():
            if exc is not None:
                logger.warning(
                    "ShareConsumer ack commit warning on %s[%d]: %s", tp.topic, tp.partition, exc
                )
        return processed

    def _step_classic(self, handler: Callable[[RawRecord], AckAction], poll_timeout: float) -> int:
        assert self._classic_consumer is not None
        msg = self._classic_consumer.poll(timeout=poll_timeout)
        if msg is None:
            return 0
        if msg.error():
            logger.debug("Classic consumer event: %s", msg.error())
            return 0

        ts_type, ts_val = msg.timestamp()
        rec = RawRecord(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key().decode("utf-8") if msg.key() is not None else None,
            value=msg.value(),
            headers=_decode_headers(msg.headers()),
            timestamp_ms=ts_val if ts_type > 0 else int(time.time() * 1000),
            delivery_count=1,
        )

        # Pause assigned partitions while running potentially slow LLM call so background poll(0)
        # keeps consumer-group heartbeats alive without fetching extra records.
        assigned = self._classic_consumer.assignment()
        stop_hb = threading.Event()
        hb_thread: threading.Thread | None = None
        if assigned:
            self._classic_consumer.pause(assigned)

            def _heartbeat_loop() -> None:
                while not stop_hb.wait(1.0):
                    try:
                        with self._consumer_lock:
                            self._classic_consumer.poll(0)
                    except Exception:
                        break

            hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
            hb_thread.start()

        try:
            decision = handler(rec)
        finally:
            stop_hb.set()
            if hb_thread is not None:
                hb_thread.join(timeout=1.5)
            if assigned and not self._classic_paused_tps:
                self._classic_consumer.resume(assigned)

        if decision in (AckAction.ACCEPT, AckAction.REJECT):
            self._classic_consumer.commit(message=msg, asynchronous=False)
            return 1

        # RELEASE in classic mode: seek back to message offset so next poll redelivers it
        self._classic_consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        return 0

    def close(self) -> None:
        if self._share_consumer is not None:
            self._share_consumer.close()
        if self._classic_consumer is not None:
            self._classic_consumer.close()

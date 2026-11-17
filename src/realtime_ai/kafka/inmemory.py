"""Thread-safe in-memory Kafka + KIP-932 Share Group simulator for fast local tests."""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field

from realtime_ai.kafka.topics import TOPICS_BY_NAME


@dataclass(frozen=True)
class StoredRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...]
    timestamp_ms: int


@dataclass
class ShareRecordState:
    status: str = "available"  # available | acquired | accepted | archived
    owner_id: str | None = None
    lock_expires_monotonic: float = 0.0
    delivery_count: int = 0


@dataclass
class TopicState:
    partitions: list[list[StoredRecord]]
    cleanup_policy: str


@dataclass
class InMemoryCluster:
    lock_duration_seconds: float = 5.0
    delivery_count_limit: int = 10
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _topics: dict[str, TopicState] = field(default_factory=dict)
    _consumer_offsets: dict[tuple[str, str, int], int] = field(default_factory=dict)
    _share_states: dict[tuple[str, str, int, int], ShareRecordState] = field(default_factory=dict)

    def _ensure_topic(self, topic: str) -> TopicState:
        with self._lock:
            if topic not in self._topics:
                spec = TOPICS_BY_NAME.get(topic)
                num_partitions = spec.partitions if spec else 3
                policy = spec.cleanup_policy if spec else "delete"
                self._topics[topic] = TopicState(
                    partitions=[[] for _ in range(num_partitions)],
                    cleanup_policy=policy,
                )
            return self._topics[topic]

    def produce(
        self,
        topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes]] | None = None,
        timestamp_ms: int | None = None,
    ) -> StoredRecord:
        with self._lock:
            tstate = self._ensure_topic(topic)
            num_p = len(tstate.partitions)
            if key is None:
                partition = sum(len(p) for p in tstate.partitions) % num_p
            else:
                digest = hashlib.md5(key, usedforsecurity=False).digest()
                partition = int.from_bytes(digest[:4], "big") % num_p
            offset = len(tstate.partitions[partition])
            rec = StoredRecord(
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
                value=value,
                headers=tuple(headers or []),
                timestamp_ms=timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
            )
            tstate.partitions[partition].append(rec)
            return rec

    def compact_topic(self, topic: str) -> None:
        """Simulates Kafka log cleaner compaction while retaining latest tombstones."""
        with self._lock:
            tstate = self._ensure_topic(topic)
            if tstate.cleanup_policy != "compact":
                return
            for idx, part in enumerate(tstate.partitions):
                latest_by_key: dict[bytes, StoredRecord] = {}
                keyless: list[StoredRecord] = []
                for rec in part:
                    if rec.key is None:
                        keyless.append(rec)
                    else:
                        latest_by_key[rec.key] = rec
                compacted = sorted(
                    [*keyless, *latest_by_key.values()], key=lambda r: r.offset
                )
                tstate.partitions[idx] = compacted

    def poll_classic(
        self,
        group_id: str,
        topics: list[str],
        max_records: int = 10,
        auto_offset_reset: str = "earliest",
    ) -> list[StoredRecord]:
        with self._lock:
            out: list[StoredRecord] = []
            for topic in topics:
                tstate = self._ensure_topic(topic)
                for p_idx, records in enumerate(tstate.partitions):
                    key = (group_id, topic, p_idx)
                    if key not in self._consumer_offsets:
                        if auto_offset_reset == "latest":
                            next_off = records[-1].offset + 1 if records else 0
                        else:
                            next_off = 0
                        self._consumer_offsets[key] = next_off
                    cur_off = self._consumer_offsets[key]
                    for rec in records:
                        if rec.offset >= cur_off:
                            out.append(rec)
                            self._consumer_offsets[key] = rec.offset + 1
                            if len(out) >= max_records:
                                return out
            return out

    def rewind_classic(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._consumer_offsets[(group_id, topic, partition)] = offset

    def poll_share(
        self,
        group_id: str,
        member_id: str,
        topics: list[str],
        max_records: int = 1,
    ) -> list[tuple[StoredRecord, int]]:
        now = time.monotonic()
        with self._lock:
            acquired: list[tuple[StoredRecord, int]] = []
            for topic in topics:
                tstate = self._ensure_topic(topic)
                for p_idx, records in enumerate(tstate.partitions):
                    for rec in records:
                        skey = (group_id, topic, p_idx, rec.offset)
                        st = self._share_states.setdefault(skey, ShareRecordState())
                        if st.status == "acquired" and now >= st.lock_expires_monotonic:
                            if st.delivery_count >= self.delivery_count_limit:
                                st.status = "archived"
                            else:
                                st.status = "available"
                                st.owner_id = None
                        if st.status == "available":
                            st.status = "acquired"
                            st.owner_id = member_id
                            st.delivery_count += 1
                            st.lock_expires_monotonic = now + self.lock_duration_seconds
                            acquired.append((rec, st.delivery_count))
                            if len(acquired) >= max_records:
                                return acquired
            return acquired

    def acknowledge_share(
        self,
        group_id: str,
        member_id: str,
        topic: str,
        partition: int,
        offset: int,
        decision: str,
    ) -> None:
        with self._lock:
            skey = (group_id, topic, partition, offset)
            st = self._share_states.get(skey)
            if st is None:
                return
            if st.owner_id not in (None, member_id):
                return
            if decision == "accept":
                st.status = "accepted"
                st.owner_id = None
            elif decision == "release":
                if st.delivery_count >= self.delivery_count_limit:
                    st.status = "archived"
                else:
                    st.status = "available"
                st.owner_id = None
            elif decision == "reject":
                st.status = "archived"
                st.owner_id = None
            elif decision == "renew":
                st.lock_expires_monotonic = time.monotonic() + self.lock_duration_seconds

    def all_records(self, topic: str) -> list[StoredRecord]:
        with self._lock:
            tstate = self._ensure_topic(topic)
            merged = [rec for part in tstate.partitions for rec in part]
            return sorted(merged, key=lambda r: (r.timestamp_ms, r.partition, r.offset))


_CLUSTERS_LOCK = threading.Lock()
_CLUSTERS: dict[str, InMemoryCluster] = {}


def get_inmemory_cluster(uri: str) -> InMemoryCluster:
    with _CLUSTERS_LOCK:
        if uri not in _CLUSTERS:
            _CLUSTERS[uri] = InMemoryCluster()
        return _CLUSTERS[uri]


def reset_inmemory_cluster(uri: str) -> InMemoryCluster:
    with _CLUSTERS_LOCK:
        _CLUSTERS[uri] = InMemoryCluster()
        return _CLUSTERS[uri]

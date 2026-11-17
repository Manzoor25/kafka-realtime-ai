"""File-system watcher / CDC document source emitting create, update, and delete tombstones."""

from __future__ import annotations

import hashlib
import logging
import re
import shutil
import time
from pathlib import Path

from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_KB_RAW
from realtime_ai.metrics import DOCS_WATCHED_TOTAL
from realtime_ai.schemas.models import KbDocumentRawEvent, now_ms

logger = logging.getLogger(__name__)

_DOC_ID_PREFIX_RE = re.compile(r"^(KB-\d{3})", re.IGNORECASE)


def derive_doc_id(path: Path) -> str:
    match = _DOC_ID_PREFIX_RE.match(path.stem)
    if match:
        return match.group(1).upper()
    return path.stem


def extract_title(path: Path, content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
    return path.stem.replace("-", " ").title()


def seed_watch_directory(seed_dir: Path, watch_dir: Path) -> int:
    watch_dir.mkdir(parents=True, exist_ok=True)
    if not seed_dir.exists():
        return 0
    copied = 0
    for src in sorted(seed_dir.glob("*.md")):
        dst = watch_dir / src.name
        if not dst.exists():
            shutil.copy2(src, dst)
            copied += 1
    return copied


class FileWatcherSource:
    """Polls `watch_dir` for .md/.txt changes and emits keyed events + delete tombstones to Kafka."""

    def __init__(self, watch_dir: Path, producer: EventProducer) -> None:
        self.watch_dir = watch_dir
        self.producer = producer
        self._known_hashes: dict[str, tuple[Path, str]] = {}

    def scan_once(self) -> int:
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        emitted = 0
        seen_doc_ids: set[str] = set()

        files = sorted(
            [p for p in self.watch_dir.iterdir() if p.is_file() and p.suffix.lower() in {".md", ".txt"}]
        )
        for path in files:
            try:
                content = path.read_text(encoding="utf-8")
            except OSError:
                continue
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            doc_id = derive_doc_id(path)
            seen_doc_ids.add(doc_id)

            prev = self._known_hashes.get(doc_id)
            if prev is not None and prev[1] == digest:
                continue

            op = "create" if prev is None else "update"
            evt = KbDocumentRawEvent(
                doc_id=doc_id,
                op=op,  # type: ignore[arg-type]
                title=extract_title(path, content),
                content=content,
                source_uri=str(path.name),
                source_ts_ms=now_ms(),
            )
            self.producer.produce_event(TOPIC_KB_RAW, key=doc_id, payload=evt, flush=True)
            self._known_hashes[doc_id] = (path, digest)
            DOCS_WATCHED_TOTAL.labels(op=op).inc()
            emitted += 1
            logger.info("Emitted %s event for doc_id=%s (%s)", op, doc_id, path.name)

        # Detect deleted files -> emit Kafka tombstones
        removed_ids = [did for did in list(self._known_hashes.keys()) if did not in seen_doc_ids]
        for doc_id in removed_ids:
            self.producer.produce_tombstone(
                TOPIC_KB_RAW,
                key=doc_id,
                headers={"source_ts_ms": str(now_ms())},
                flush=True,
            )
            del self._known_hashes[doc_id]
            DOCS_WATCHED_TOTAL.labels(op="delete").inc()
            emitted += 1
            logger.info("Emitted delete tombstone for doc_id=%s", doc_id)

        return emitted

    def run_forever(self, poll_seconds: float = 0.4) -> None:
        logger.info("Watching KB directory %s (poll=%.2fs)", self.watch_dir, poll_seconds)
        while True:
            self.scan_once()
            time.sleep(poll_seconds)

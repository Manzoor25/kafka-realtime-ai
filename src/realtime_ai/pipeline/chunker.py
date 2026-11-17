"""Deterministic markdown-aware overlapping chunker with SHA-256 content addressing."""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass

CHUNK_NAMESPACE = uuid.UUID("7f9d3a10-6c44-4d89-9a4e-2f7b11c8e501")
_HEADING_SPLIT_RE = re.compile(r"(?m)^(?=#{1,3}\s+)")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class DocumentChunk:
    doc_id: str
    chunk_index: int
    chunk_hash: str
    point_id: str
    title: str
    section: str
    text: str
    embedding_model: str
    embedding_version: str


def compute_chunk_hash(text: str) -> str:
    canonical = _WHITESPACE_RE.sub(" ", text.strip())
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def deterministic_point_id(doc_id: str, chunk_hash: str, embedding_version: str) -> str:
    """Deterministic UUIDv5 required by Qdrant so replays/duplicates overwrite in place."""
    key = f"{doc_id}:{chunk_hash}:{embedding_version}"
    return str(uuid.uuid5(CHUNK_NAMESPACE, key))


def _window_split(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    clean = text.strip()
    if not clean:
        return []
    if len(clean) <= max_chars:
        return [clean]

    step = max(1, max_chars - max(0, overlap_chars))
    windows: list[str] = []
    start = 0
    n = len(clean)
    while start < n:
        end = min(n, start + max_chars)
        if end < n:
            # Prefer breaking at sentence or word boundary inside the last 25% of window
            floor = start + int(max_chars * 0.75)
            boundary = max(
                clean.rfind(". ", floor, end),
                clean.rfind("\n", floor, end),
                clean.rfind(" ", floor, end),
            )
            if boundary > start:
                end = boundary + 1
        piece = clean[start:end].strip()
        if piece and (not windows or piece != windows[-1]):
            windows.append(piece)
        if end >= n:
            break
        start = max(start + 1, end - overlap_chars if overlap_chars > 0 else end)
        # Ensure forward progress
        if start <= end - max_chars:
            start += step
    return windows


def chunk_document(
    *,
    doc_id: str,
    title: str,
    sanitized_content: str,
    embedding_model: str,
    embedding_version: str,
    max_chars: int = 520,
    overlap_chars: int = 80,
) -> list[DocumentChunk]:
    """Splits sanitized markdown/text by section headings then overlapping windows."""
    if not sanitized_content or not sanitized_content.strip():
        return []

    raw_sections = [s.strip() for s in _HEADING_SPLIT_RE.split(sanitized_content) if s.strip()]
    chunks: list[DocumentChunk] = []
    seen_hashes: set[str] = set()
    idx = 0

    for sec in raw_sections:
        lines = sec.splitlines()
        if lines and lines[0].lstrip().startswith("#"):
            section_title = lines[0].lstrip("#").strip()
            body = "\n".join(lines[1:]).strip() or section_title
        else:
            section_title = title
            body = sec

        for piece in _window_split(body, max_chars=max_chars, overlap_chars=overlap_chars):
            contextual_text = f"{title} — {section_title}\n{piece}" if section_title != title else f"{title}\n{piece}"
            chash = compute_chunk_hash(contextual_text)
            if chash in seen_hashes:
                continue
            seen_hashes.add(chash)
            pid = deterministic_point_id(doc_id, chash, embedding_version)
            chunks.append(
                DocumentChunk(
                    doc_id=doc_id,
                    chunk_index=idx,
                    chunk_hash=chash,
                    point_id=pid,
                    title=title,
                    section=section_title,
                    text=contextual_text,
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                )
            )
            idx += 1

    return chunks

"""Unit tests for markdown-aware chunking, SHA-256 content hashes, and UUIDv5 point IDs."""

from __future__ import annotations

import uuid

from realtime_ai.pipeline.chunker import (
    chunk_document,
    compute_chunk_hash,
    deterministic_point_id,
)


def test_chunk_hash_normalizes_whitespace_deterministically() -> None:
    h1 = compute_chunk_hash("Reset   SAML   certificate\nin Okta")
    h2 = compute_chunk_hash("  Reset SAML certificate in Okta  ")
    assert h1 == h2
    assert len(h1) == 24


def test_deterministic_point_id_is_valid_uuid5_and_isolated_by_version() -> None:
    chash = compute_chunk_hash("Rotate webhook secret via CLI")
    pid_v1_first = deterministic_point_id("KB-002", chash, "v1")
    pid_v1_second = deterministic_point_id("KB-002", chash, "v1")
    pid_v2 = deterministic_point_id("KB-002", chash, "v2")
    pid_other_doc = deterministic_point_id("KB-003", chash, "v1")

    parsed = uuid.UUID(pid_v1_first)
    assert parsed.version == 5
    assert pid_v1_first == pid_v1_second
    assert pid_v1_first != pid_v2
    assert pid_v1_first != pid_other_doc


def test_chunk_document_splits_markdown_sections_and_deduplicates_identical_blocks() -> None:
    markdown = (
        "# SSO Troubleshooting\n"
        "Verify IdP entity ID matches SP audience URI.\n\n"
        "## Clock Skew\n"
        + ("SAML assertions fail if NTP drift exceeds 120 seconds across nodes. " * 14)
        + "\n\n## Duplicate Section\n"
        "Verify IdP entity ID matches SP audience URI.\n"
    )
    chunks = chunk_document(
        doc_id="KB-101",
        title="SSO Troubleshooting",
        sanitized_content=markdown,
        embedding_model="all-minilm:22m",
        embedding_version="v1",
        max_chars=260,
        overlap_chars=50,
    )
    assert len(chunks) >= 4
    assert [c.chunk_index for c in chunks] == list(range(len(chunks)))
    assert chunks[0].section == "SSO Troubleshooting"
    assert any(c.section == "Clock Skew" for c in chunks)

    # Re-chunking the exact same document yields identical chunk hashes and point IDs
    chunks_again = chunk_document(
        doc_id="KB-101",
        title="SSO Troubleshooting",
        sanitized_content=markdown,
        embedding_model="all-minilm:22m",
        embedding_version="v1",
        max_chars=260,
        overlap_chars=50,
    )
    assert [c.point_id for c in chunks] == [c.point_id for c in chunks_again]
    assert [c.chunk_hash for c in chunks] == [c.chunk_hash for c in chunks_again]


def test_chunk_document_empty_or_whitespace_returns_empty_list() -> None:
    assert (
        chunk_document(
            doc_id="KB-EMPTY",
            title="Empty",
            sanitized_content="   \n\t  ",
            embedding_model="all-minilm:22m",
            embedding_version="v1",
        )
        == []
    )

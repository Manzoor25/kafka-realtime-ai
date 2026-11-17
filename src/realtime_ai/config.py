"""Centralized typed configuration loaded from environment variables or .env."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka & Schema Registry
    kafka_bootstrap_servers: str = Field(
        default="localhost:19092,localhost:19093,localhost:19094"
    )
    schema_registry_url: str = Field(default="http://localhost:8081")
    kafka_consumer_mode: Literal["share", "classic"] = Field(default="share")
    share_group_triage: str = Field(default="sg-triage-workers")
    worker_id: str = Field(default="worker-local-1")

    # Paths
    schemas_dir: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[2] / "schemas"
    )
    seed_kb_dir: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[2] / "data" / "kb"
    )
    watch_dir: Path = Field(default=Path("./watched_kb"))
    watch_poll_seconds: float = Field(default=0.4)

    # Vector Store (Qdrant)
    qdrant_url: str = Field(default="http://localhost:6333")
    qdrant_local_path: str | None = Field(default=None)
    qdrant_active_alias: str = Field(default="kb_chunks_active")
    embedding_model: str = Field(default="all-minilm:22m")
    embedding_version: str = Field(default="v1")
    embedding_dimensions: int = Field(default=384)
    chunk_size_chars: int = Field(default=520)
    chunk_overlap_chars: int = Field(default=80)

    # AI Provider
    ai_provider: Literal["ollama", "local", "openai"] = Field(default="ollama")
    ollama_base_url: str = Field(default="http://localhost:11434")
    chat_model: str = Field(default="qwen2.5:0.5b")
    openai_base_url: str = Field(default="https://api.openai.com/v1")
    openai_api_key: str = Field(default="")

    # Resilience & Governance
    llm_timeout_seconds: float = Field(default=18.0)
    llm_max_retries: int = Field(default=4)
    llm_backoff_base_seconds: float = Field(default=0.75)
    llm_backoff_max_seconds: float = Field(default=8.0)
    llm_rate_limit_rps: float = Field(default=8.0)
    provider_outage_probe_seconds: float = Field(default=2.0)
    audit_retention_days: int = Field(default=2555)  # 7 years compliance retention
    max_draft_revisions: int = Field(default=2)

    # Observability
    metrics_port: int = Field(default=0)
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)

    def collection_name_for(self, model: str | None = None, version: str | None = None) -> str:
        raw = f"kb_chunks__{model or self.embedding_model}__{version or self.embedding_version}"
        safe = "".join(ch if ch.isalnum() else "_" for ch in raw.lower())
        while "__" in safe:
            safe = safe.replace("__", "_")
        return safe.strip("_")

"""Resilience wrapper around AIProviderBackend: timeouts, backoff retries, rate-limit pause, and audit log."""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any

from realtime_ai.config import Settings
from realtime_ai.kafka.consumer import WorkerConsumer
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import TOPIC_AI_AUDIT
from realtime_ai.metrics import (
    LLM_CALL_LATENCY_SECONDS,
    LLM_CALLS_TOTAL,
    LLM_RETRIES_TOTAL,
)
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.providers.base import (
    AIProviderBackend,
    ChatResult,
    EmbeddingResult,
    ProviderRateLimitError,
    TransientProviderError,
)
from realtime_ai.schemas.models import AiAuditEvent

logger = logging.getLogger(__name__)

# Cloud-equivalent / shadow reference unit prices per 1M tokens (USD) for cost governance
MODEL_PRICING_PER_1M_TOKENS_USD: dict[str, tuple[float, float]] = {
    # (prompt_usd_per_1m, completion_usd_per_1m)
    "all-minilm:22m": (0.02, 0.0),
    "nomic-embed-text:v1.5": (0.04, 0.0),
    "qwen2.5:0.5b": (0.10, 0.30),
    "local-hash-v1": (0.02, 0.0),
    "local-semantic-v2": (0.04, 0.0),
    "text-embedding-3-small": (0.02, 0.0),
    "gpt-4o-mini": (0.15, 0.60),
}


def estimate_cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    in_rate, out_rate = MODEL_PRICING_PER_1M_TOKENS_USD.get(model, (0.12, 0.36))
    cost = (prompt_tokens * in_rate + completion_tokens * out_rate) / 1_000_000.0
    return round(cost, 8)


class TokenBucketRateLimiter:
    """Thread-safe token bucket limiter preventing downstream LLM burst saturation."""

    def __init__(self, rate_per_second: float, capacity: float | None = None) -> None:
        self._rate = max(0.1, rate_per_second)
        self._capacity = capacity if capacity is not None else max(2.0, rate_per_second * 2.0)
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume_or_wait_time(self, cost: float = 1.0) -> float:
        """Consumes a token immediately (returns 0.0) or returns seconds required to wait."""
        with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self._last_refill)
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = now
            if self._tokens >= cost:
                self._tokens -= cost
                return 0.0
            needed = cost - self._tokens
            return needed / self._rate


class ResilientAIProvider:
    """Executes embed/chat calls with rate limiting, retries, consumer pause hooks, and Kafka audit logging."""

    def __init__(
        self,
        backend: AIProviderBackend,
        settings: Settings,
        audit_producer: EventProducer | None = None,
    ) -> None:
        self.backend = backend
        self.settings = settings
        self._audit_producer = audit_producer
        self._limiter = TokenBucketRateLimiter(settings.llm_rate_limit_rps)

    @property
    def provider_name(self) -> str:
        return self.backend.provider_name

    def health_check(self) -> bool:
        return self.backend.health_check(timeout_seconds=2.0)

    def _throttle(self, consumer: WorkerConsumer | None) -> None:
        wait_s = self._limiter.consume_or_wait_time(1.0)
        if wait_s > 0.0:
            if consumer is not None:
                consumer.pause_for(wait_s, reason="local_rate_limit")
            time.sleep(wait_s)

    def embed(
        self,
        texts: list[str],
        *,
        model: str | None = None,
        stage: str = "indexer",
        agent_name: str = "kb-indexer",
        correlation_id: str = "batch",
        consumer: WorkerConsumer | None = None,
    ) -> EmbeddingResult:
        chosen_model = model or self.settings.embedding_model
        attempts = 0
        start = time.perf_counter()

        while True:
            self._throttle(consumer)
            call_t0 = time.perf_counter()
            try:
                result = self.backend.embed(
                    texts,
                    model=chosen_model,
                    timeout_seconds=self.settings.llm_timeout_seconds,
                )
                elapsed = time.perf_counter() - call_t0
                LLM_CALL_LATENCY_SECONDS.labels(
                    provider=self.provider_name, operation="embed", stage=stage
                ).observe(elapsed)
                LLM_CALLS_TOTAL.labels(
                    provider=self.provider_name,
                    model=chosen_model,
                    operation="embed",
                    stage=stage,
                    status="ok",
                ).inc()
                self._emit_audit(
                    correlation_id=correlation_id,
                    stage=stage,
                    agent_name=agent_name,
                    operation="embed",
                    model_version=chosen_model,
                    prompt_raw="\n---\n".join(texts)[:2000],
                    response_text=f"embedded_chunks={len(result.vectors)} dim={len(result.vectors[0]) if result.vectors else 0}",
                    prompt_tokens=result.prompt_tokens,
                    completion_tokens=0,
                    latency_ms=(time.perf_counter() - start) * 1000.0,
                    retry_count=attempts,
                    status="ok",
                )
                return result
            except TransientProviderError as exc:
                attempts += 1
                reason = type(exc).__name__
                LLM_RETRIES_TOTAL.labels(
                    provider=self.provider_name,
                    operation="embed",
                    stage=stage,
                    reason=reason,
                ).inc()
                if isinstance(exc, ProviderRateLimitError) and consumer is not None:
                    consumer.pause_for(exc.retry_after_seconds, reason="http_429")
                if attempts > self.settings.llm_max_retries:
                    LLM_CALLS_TOTAL.labels(
                        provider=self.provider_name,
                        model=chosen_model,
                        operation="embed",
                        stage=stage,
                        status="error",
                    ).inc()
                    self._emit_audit(
                        correlation_id=correlation_id,
                        stage=stage,
                        agent_name=agent_name,
                        operation="embed",
                        model_version=chosen_model,
                        prompt_raw="\n---\n".join(texts)[:2000],
                        response_text=f"ERROR({reason}): {exc}",
                        prompt_tokens=0,
                        completion_tokens=0,
                        latency_ms=(time.perf_counter() - start) * 1000.0,
                        retry_count=attempts,
                        status="error",
                    )
                    raise
                sleep_s = self._compute_backoff(attempts, exc)
                logger.warning(
                    "Transient embed error (%s) on attempt %d/%d; retrying in %.2fs",
                    exc,
                    attempts,
                    self.settings.llm_max_retries,
                    sleep_s,
                )
                time.sleep(sleep_s)
            except Exception as exc:
                LLM_CALLS_TOTAL.labels(
                    provider=self.provider_name,
                    model=chosen_model,
                    operation="embed",
                    stage=stage,
                    status="error",
                ).inc()
                self._emit_audit(
                    correlation_id=correlation_id,
                    stage=stage,
                    agent_name=agent_name,
                    operation="embed",
                    model_version=chosen_model,
                    prompt_raw="\n---\n".join(texts)[:2000],
                    response_text=f"PERMANENT_ERROR({type(exc).__name__}): {exc}",
                    prompt_tokens=0,
                    completion_tokens=0,
                    latency_ms=(time.perf_counter() - start) * 1000.0,
                    retry_count=attempts,
                    status="error",
                )
                raise

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str | None = None,
        stage: str,
        agent_name: str,
        correlation_id: str,
        json_schema: dict[str, Any] | None = None,
        consumer: WorkerConsumer | None = None,
    ) -> ChatResult:
        chosen_model = model or self.settings.chat_model
        attempts = 0
        start = time.perf_counter()

        while True:
            self._throttle(consumer)
            call_t0 = time.perf_counter()
            try:
                result = self.backend.chat(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    model=chosen_model,
                    timeout_seconds=self.settings.llm_timeout_seconds,
                    json_schema=json_schema,
                )
                elapsed = time.perf_counter() - call_t0
                LLM_CALL_LATENCY_SECONDS.labels(
                    provider=self.provider_name, operation="chat", stage=stage
                ).observe(elapsed)
                LLM_CALLS_TOTAL.labels(
                    provider=self.provider_name,
                    model=chosen_model,
                    operation="chat",
                    stage=stage,
                    status="ok",
                ).inc()
                self._emit_audit(
                    correlation_id=correlation_id,
                    stage=stage,
                    agent_name=agent_name,
                    operation="chat",
                    model_version=chosen_model,
                    prompt_raw=f"[SYS] {system_prompt}\n[USR] {user_prompt}"[:3000],
                    response_text=result.text[:3000],
                    prompt_tokens=result.prompt_tokens,
                    completion_tokens=result.completion_tokens,
                    latency_ms=(time.perf_counter() - start) * 1000.0,
                    retry_count=attempts,
                    status="ok",
                )
                return result
            except TransientProviderError as exc:
                attempts += 1
                reason = type(exc).__name__
                LLM_RETRIES_TOTAL.labels(
                    provider=self.provider_name,
                    operation="chat",
                    stage=stage,
                    reason=reason,
                ).inc()
                if isinstance(exc, ProviderRateLimitError) and consumer is not None:
                    consumer.pause_for(exc.retry_after_seconds, reason="http_429")
                if attempts > self.settings.llm_max_retries:
                    LLM_CALLS_TOTAL.labels(
                        provider=self.provider_name,
                        model=chosen_model,
                        operation="chat",
                        stage=stage,
                        status="error",
                    ).inc()
                    self._emit_audit(
                        correlation_id=correlation_id,
                        stage=stage,
                        agent_name=agent_name,
                        operation="chat",
                        model_version=chosen_model,
                        prompt_raw=f"[SYS] {system_prompt}\n[USR] {user_prompt}"[:3000],
                        response_text=f"ERROR({reason}): {exc}",
                        prompt_tokens=0,
                        completion_tokens=0,
                        latency_ms=(time.perf_counter() - start) * 1000.0,
                        retry_count=attempts,
                        status="error",
                    )
                    raise
                sleep_s = self._compute_backoff(attempts, exc)
                logger.warning(
                    "Transient chat error (%s) on attempt %d/%d; retrying in %.2fs",
                    exc,
                    attempts,
                    self.settings.llm_max_retries,
                    sleep_s,
                )
                time.sleep(sleep_s)
            except Exception as exc:
                LLM_CALLS_TOTAL.labels(
                    provider=self.provider_name,
                    model=chosen_model,
                    operation="chat",
                    stage=stage,
                    status="error",
                ).inc()
                self._emit_audit(
                    correlation_id=correlation_id,
                    stage=stage,
                    agent_name=agent_name,
                    operation="chat",
                    model_version=chosen_model,
                    prompt_raw=f"[SYS] {system_prompt}\n[USR] {user_prompt}"[:3000],
                    response_text=f"PERMANENT_ERROR({type(exc).__name__}): {exc}",
                    prompt_tokens=0,
                    completion_tokens=0,
                    latency_ms=(time.perf_counter() - start) * 1000.0,
                    retry_count=attempts,
                    status="error",
                )
                raise

    def _compute_backoff(self, attempt: int, exc: TransientProviderError) -> float:
        if isinstance(exc, ProviderRateLimitError):
            return min(self.settings.llm_backoff_max_seconds, max(0.1, exc.retry_after_seconds))
        exp = self.settings.llm_backoff_base_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self.settings.llm_backoff_base_seconds * 0.35)
        return min(self.settings.llm_backoff_max_seconds, exp + jitter)

    def _emit_audit(
        self,
        *,
        correlation_id: str,
        stage: str,
        agent_name: str,
        operation: str,
        model_version: str,
        prompt_raw: str,
        response_text: str,
        prompt_tokens: int,
        completion_tokens: int,
        latency_ms: float,
        retry_count: int,
        status: str,
    ) -> None:
        if self._audit_producer is None:
            return
        sanitized_prompt = redact_pii(prompt_raw).sanitized_text
        evt = AiAuditEvent(
            correlation_id=correlation_id,
            stage=stage,
            agent_name=agent_name,
            operation=operation,  # type: ignore[arg-type]
            provider=self.provider_name,
            model_version=model_version,
            prompt_redacted=sanitized_prompt,
            response_text=response_text,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            estimated_cost_usd=estimate_cost_usd(model_version, prompt_tokens, completion_tokens),
            latency_ms=round(latency_ms, 2),
            retry_count=retry_count,
            status=status,  # type: ignore[arg-type]
        )
        try:
            self._audit_producer.produce_event(TOPIC_AI_AUDIT, key=correlation_id, payload=evt)
        except Exception as exc:
            logger.warning("Failed to publish audit event: %s", exc)

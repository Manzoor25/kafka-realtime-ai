"""Small provider protocol and error taxonomy for embeddings and chat completions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class ProviderError(Exception):
    """Base error raised by embedding or LLM providers."""


class TransientProviderError(ProviderError):
    """Retriable error (timeout, connection failure, 5xx, 429)."""


class ProviderTimeoutError(TransientProviderError):
    """Provider request exceeded configured per-call deadline."""


class ProviderUnavailableError(TransientProviderError):
    """Provider process/network unreachable or returned 5xx."""


class ProviderRateLimitError(TransientProviderError):
    """Provider returned HTTP 429 or local rate budget requested backoff."""

    def __init__(self, message: str, retry_after_seconds: float = 2.0) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ProviderPermanentError(ProviderError):
    """Non-retriable error (e.g., invalid request / poison input) destined for DLQ."""


@dataclass(frozen=True)
class EmbeddingResult:
    vectors: list[list[float]]
    model: str
    prompt_tokens: int


@dataclass(frozen=True)
class ChatResult:
    text: str
    model: str
    prompt_tokens: int
    completion_tokens: int

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class AIProviderBackend(Protocol):
    provider_name: str

    def embed(
        self, texts: list[str], model: str, timeout_seconds: float = 15.0
    ) -> EmbeddingResult: ...

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_seconds: float = 18.0,
        json_schema: dict[str, Any] | None = None,
    ) -> ChatResult: ...

    def health_check(self, timeout_seconds: float = 2.0) -> bool: ...

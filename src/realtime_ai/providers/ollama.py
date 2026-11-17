"""Local Ollama HTTP provider (runs 100% locally with zero API keys)."""

from __future__ import annotations

from typing import Any

import httpx

from realtime_ai.providers.base import (
    ChatResult,
    EmbeddingResult,
    ProviderPermanentError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
)


def _estimate_tokens(text: str) -> int:
    return max(1, len(text.split()) * 4 // 3)


class OllamaProvider:
    provider_name = "ollama"

    def __init__(self, base_url: str = "http://localhost:11434") -> None:
        self._base_url = base_url.rstrip("/")

    def embed(
        self, texts: list[str], model: str, timeout_seconds: float = 15.0
    ) -> EmbeddingResult:
        if not texts:
            return EmbeddingResult(vectors=[], model=model, prompt_tokens=0)
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.post(
                    f"{self._base_url}/api/embed",
                    json={"model": model, "input": texts},
                )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(f"Ollama embed timed out after {timeout_seconds}s") from exc
        except httpx.RequestError as exc:
            raise ProviderUnavailableError(f"Ollama unreachable at {self._base_url}: {exc}") from exc

        self._check_status(resp)
        data = resp.json()
        vectors = data.get("embeddings") or []
        if not vectors or len(vectors) != len(texts):
            raise ProviderUnavailableError(
                f"Ollama returned {len(vectors)} vectors for {len(texts)} inputs"
            )
        prompt_tokens = int(
            data.get("prompt_eval_count") or sum(_estimate_tokens(t) for t in texts)
        )
        return EmbeddingResult(vectors=vectors, model=model, prompt_tokens=prompt_tokens)

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_seconds: float = 18.0,
        json_schema: dict[str, Any] | None = None,
    ) -> ChatResult:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 260},
        }
        if json_schema is not None:
            payload["format"] = json_schema

        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.post(f"{self._base_url}/api/chat", json=payload)
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(f"Ollama chat timed out after {timeout_seconds}s") from exc
        except httpx.RequestError as exc:
            raise ProviderUnavailableError(f"Ollama unreachable at {self._base_url}: {exc}") from exc

        self._check_status(resp)
        data = resp.json()
        content = (data.get("message") or {}).get("content") or ""
        prompt_tokens = int(
            data.get("prompt_eval_count")
            or _estimate_tokens(system_prompt) + _estimate_tokens(user_prompt)
        )
        completion_tokens = int(data.get("eval_count") or _estimate_tokens(content))
        return ChatResult(
            text=content,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def health_check(self, timeout_seconds: float = 2.0) -> bool:
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.get(f"{self._base_url}/api/tags")
                return resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _check_status(resp: httpx.Response) -> None:
        if resp.status_code < 400:
            return
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "2.0"))
            raise ProviderRateLimitError("Ollama returned HTTP 429", retry_after_seconds=retry_after)
        if resp.status_code >= 500:
            raise ProviderUnavailableError(
                f"Ollama HTTP {resp.status_code}: {resp.text[:300]}"
            )
        raise ProviderPermanentError(f"Ollama HTTP {resp.status_code}: {resp.text[:300]}")

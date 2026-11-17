"""OpenAI-compatible hosted provider (OpenAI, Azure OpenAI, vLLM, Groq, Fireworks) via config."""

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


class OpenAICompatibleProvider:
    provider_name = "openai"

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def embed(
        self, texts: list[str], model: str, timeout_seconds: float = 15.0
    ) -> EmbeddingResult:
        if not texts:
            return EmbeddingResult(vectors=[], model=model, prompt_tokens=0)
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.post(
                    f"{self._base_url}/embeddings",
                    headers=self._headers(),
                    json={"model": model, "input": texts},
                )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(f"OpenAI embed timed out after {timeout_seconds}s") from exc
        except httpx.RequestError as exc:
            raise ProviderUnavailableError(f"OpenAI endpoint unreachable: {exc}") from exc

        self._check_status(resp)
        body = resp.json()
        items = sorted(body.get("data", []), key=lambda item: item["index"])
        vectors = [item["embedding"] for item in items]
        usage = body.get("usage") or {}
        return EmbeddingResult(
            vectors=vectors,
            model=model,
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
        )

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
            "temperature": 0.1,
        }
        if json_schema is not None:
            payload["response_format"] = {"type": "json_object"}

        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.post(
                    f"{self._base_url}/chat/completions",
                    headers=self._headers(),
                    json=payload,
                )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(f"OpenAI chat timed out after {timeout_seconds}s") from exc
        except httpx.RequestError as exc:
            raise ProviderUnavailableError(f"OpenAI endpoint unreachable: {exc}") from exc

        self._check_status(resp)
        body = resp.json()
        text = body["choices"][0]["message"]["content"] or ""
        usage = body.get("usage") or {}
        return ChatResult(
            text=text,
            model=model,
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
            completion_tokens=int(usage.get("completion_tokens", 0)),
        )

    def health_check(self, timeout_seconds: float = 2.0) -> bool:
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                resp = client.get(f"{self._base_url}/models", headers=self._headers())
                return resp.status_code < 500
        except Exception:
            return False

    @staticmethod
    def _check_status(resp: httpx.Response) -> None:
        if resp.status_code < 400:
            return
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "2.0"))
            raise ProviderRateLimitError("Hosted provider HTTP 429", retry_after_seconds=retry_after)
        if resp.status_code >= 500:
            raise ProviderUnavailableError(
                f"Hosted provider HTTP {resp.status_code}: {resp.text[:300]}"
            )
        raise ProviderPermanentError(
            f"Hosted provider HTTP {resp.status_code}: {resp.text[:300]}"
        )

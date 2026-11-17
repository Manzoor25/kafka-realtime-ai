"""Factory instantiating the configured AI provider backend wrapped in ResilientAIProvider."""

from __future__ import annotations

from realtime_ai.config import Settings
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.providers.base import AIProviderBackend
from realtime_ai.providers.local_cpu import LocalCPUProvider
from realtime_ai.providers.ollama import OllamaProvider
from realtime_ai.providers.openai_compat import OpenAICompatibleProvider
from realtime_ai.providers.resilient import ResilientAIProvider


def create_provider_backend(settings: Settings) -> AIProviderBackend:
    if settings.ai_provider == "ollama":
        return OllamaProvider(base_url=settings.ollama_base_url)
    if settings.ai_provider == "local":
        return LocalCPUProvider()
    if settings.ai_provider == "openai":
        return OpenAICompatibleProvider(
            base_url=settings.openai_base_url,
            api_key=settings.openai_api_key,
        )
    raise ValueError(f"Unsupported AI_PROVIDER='{settings.ai_provider}'")


def create_resilient_provider(
    settings: Settings, audit_producer: EventProducer | None = None
) -> ResilientAIProvider:
    backend = create_provider_backend(settings)
    return ResilientAIProvider(backend=backend, settings=settings, audit_producer=audit_producer)

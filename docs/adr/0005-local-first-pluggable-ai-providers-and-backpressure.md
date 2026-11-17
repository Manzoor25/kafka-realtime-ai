# ADR-0005: Local-First Pluggable AI Provider Interface with Consumer-Aware Backpressure

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
A portfolio platform project must run out-of-the-box with **zero API keys**, switch to a hosted provider (OpenAI, Azure OpenAI, vLLM) via environment variables alone, and survive real-world LLM failure modes: timeouts, HTTP 429 rate limits, and multi-minute provider outages without dropping customer tickets.

## Decision
1. Define a minimal `AIProviderBackend` protocol (`embed`, `chat`, `health_check`) with three implementations:
   - **`OllamaProvider`** (default in `docker-compose.yml` via `ollama/ollama:0.40.16`): Runs `all-minilm:22m` (384-dim), `nomic-embed-text:v1.5` (768-dim for live replay cutover), and `qwen2.5:0.5b` completely locally on CPU/GPU.
   - **`OpenAICompatibleProvider`** (`AI_PROVIDER=openai`): Targets any `/v1/embeddings` + `/v1/chat/completions` endpoint via `OPENAI_BASE_URL` and `OPENAI_API_KEY`.
   - **`LocalCPUProvider`** (`AI_PROVIDER=local`): Zero-network deterministic CPU backend with lexical (`v1`, 384-dim) and synonym-expanded semantic (`v2`, 768-dim) embeddings plus fault-injection hooks for sub-second hermetic CI tests.
2. Wrap backends in **`ResilientAIProvider`**, enforcing:
   - Per-call timeouts (`LLM_TIMEOUT_SECONDS`).
   - Exponential backoff with jitter (`LLM_MAX_RETRIES`, `Retry-After` header support).
   - Token-bucket rate limiting (`LLM_RATE_LIMIT_RPS`) coupled to `WorkerConsumer.pause_for()` (calling librdkafka `consumer.pause(assignment)` / `resume(assignment)` on classic consumers and poll-gating on share consumers).
   - Dead-letter routing (`dlq.events.v1`) only for schema/permanent errors or exhausted `share.delivery.count.limit`, never for transient LLM outages.

## Consequences
- **Positive:** Clone-and-run ergonomics with zero cloud credentials; swapping to GPT-4o-mini or vLLM requires zero code edits; 2-minute LLM outages result in `0` lost tickets.
- **Negative:** Small CPU-friendly local models (`qwen2.5:0.5b`) have lower reasoning nuance than frontier hosted models, so agents pair structured JSON prompts with deterministic guardrails.

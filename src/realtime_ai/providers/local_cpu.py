"""Zero-daemon deterministic CPU provider for offline runs, unit tests, and fault injection."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from typing import Any

from realtime_ai.providers.base import (
    ChatResult,
    EmbeddingResult,
    ProviderPermanentError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
)

_STOPWORDS = frozenset(
    {
        "a", "an", "and", "are", "as", "at", "be", "by", "can", "do", "for", "from", "how",
        "i", "in", "is", "it", "my", "of", "on", "or", "our", "the", "this", "to", "was",
        "we", "what", "when", "where", "who", "why", "with", "you", "your",
    }
)

# Domain concept clusters bridged only by v2 semantic model (not v1 lexical hash)
_CONCEPT_CLUSTERS: tuple[tuple[str, frozenset[str]], ...] = (
    (
        "concept_sso_saml",
        frozenset({"sso", "saml", "idp", "okta", "entra", "assertion", "acs", "redirect", "login", "signon", "certificate", "clock", "skew"}),
    ),
    (
        "concept_webhook_hmac",
        frozenset({"webhook", "webhooks", "hmac", "signature", "sha256", "signing", "secret", "callback", "payload", "rotation", "dual"}),
    ),
    (
        "concept_rate_limit",
        frozenset({"429", "rate", "limit", "limiting", "throttle", "throttled", "backoff", "jitter", "quota", "burst", "rps", "retry"}),
    ),
    (
        "concept_billing_vat",
        frozenset({"invoice", "vat", "tax", "refund", "prorated", "credit", "billing", "charge", "overcharged", "receipt", "eu"}),
    ),
    (
        "concept_kafka_share",
        frozenset({"share", "kip932", "kip", "932", "queue", "lock", "acknowledge", "redelivery", "partition", "partitions", "workers"}),
    ),
    (
        "concept_gdpr_privacy",
        frozenset({"gdpr", "erasure", "forget", "forgotten", "deletion", "delete", "dsar", "pii", "personal", "privacy", "compliance", "sla"}),
    ),
    (
        "concept_key_compromise",
        frozenset({"compromised", "compromise", "leaked", "exposed", "github", "revoke", "revocation", "incident", "token", "key", "secret"}),
    ),
    (
        "concept_scim_sync",
        frozenset({"scim", "provisioning", "deprovisioning", "group", "groups", "membership", "directory", "sync", "delayed", "stale"}),
    ),
)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_DOC_CITATION_RE = re.compile(r"\b(KB-\d{3}[A-Za-z0-9_-]*)\b")


def _stem(word: str) -> str:
    for suffix in ("ing", "tion", "ions", "ed", "es", "s"):
        if len(word) > len(suffix) + 3 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def _hash_feature(feature: str, dims: int) -> tuple[int, float]:
    digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
    val = int.from_bytes(digest, "big")
    idx = val % dims
    sign = 1.0 if ((val >> 32) & 1) == 0 else -1.0
    return idx, sign


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    if norm <= 1e-12:
        vec[0] = 1.0
        return vec
    return [round(v / norm, 7) for v in vec]


class LocalCPUProvider:
    """Deterministic local CPU embedding + support chat provider with fault-injection controls."""

    provider_name = "local"

    def __init__(self) -> None:
        self.outage_active: bool = False
        self.simulated_latency_seconds: float = 0.0
        self.remaining_rate_limits: int = 0
        self.rate_limit_retry_after: float = 0.5

    @staticmethod
    def dimensions_for_model(model: str) -> int:
        m = model.lower()
        if "nomic" in m or "v2" in m or "768" in m:
            return 768
        return 384

    @staticmethod
    def _is_v2_semantic(model: str) -> bool:
        m = model.lower()
        return "nomic" in m or "v2" in m or "semantic" in m

    def _maybe_fault(self, timeout_seconds: float, prompt_text: str = "") -> None:
        if "POISON_PERMANENT_ERROR" in prompt_text:
            raise ProviderPermanentError("Simulated permanent poison payload error")
        if self.outage_active:
            raise ProviderUnavailableError("LocalCPUProvider simulated outage is active")
        if self.simulated_latency_seconds > 0:
            if self.simulated_latency_seconds > timeout_seconds:
                time.sleep(timeout_seconds)
                raise ProviderTimeoutError(
                    f"Simulated latency {self.simulated_latency_seconds}s exceeded timeout {timeout_seconds}s"
                )
            time.sleep(self.simulated_latency_seconds)
        if self.remaining_rate_limits > 0:
            self.remaining_rate_limits -= 1
            raise ProviderRateLimitError(
                "Simulated HTTP 429 Too Many Requests",
                retry_after_seconds=self.rate_limit_retry_after,
            )

    def embed(
        self, texts: list[str], model: str, timeout_seconds: float = 15.0
    ) -> EmbeddingResult:
        self._maybe_fault(timeout_seconds, " ".join(texts[:1]))
        dims = self.dimensions_for_model(model)
        is_v2 = self._is_v2_semantic(model)
        vectors: list[list[float]] = []
        total_tokens = 0

        for text in texts:
            raw_tokens = [t for t in _TOKEN_RE.findall(text.lower()) if t not in _STOPWORDS]
            total_tokens += max(1, len(raw_tokens))
            vec = [0.0] * dims

            if not is_v2:
                # v1 baseline: strict surface-word lexical feature hashing only
                for tok in raw_tokens:
                    idx, sign = _hash_feature(f"w:{tok}", dims)
                    vec[idx] += sign * 1.0
            else:
                # v2 upgraded semantic representation: stems + char n-grams + domain concept clusters
                stems = [_stem(t) for t in raw_tokens]
                token_set = set(raw_tokens) | set(stems)
                for tok, st in zip(raw_tokens, stems, strict=False):
                    i1, s1 = _hash_feature(f"w:{tok}", dims)
                    vec[i1] += s1 * 1.0
                    i2, s2 = _hash_feature(f"st:{st}", dims)
                    vec[i2] += s2 * 1.15
                    if len(st) >= 4:
                        for pos in range(len(st) - 2):
                            tri = st[pos : pos + 3]
                            i3, s3 = _hash_feature(f"g3:{tri}", dims)
                            vec[i3] += s3 * 0.28
                for concept_name, vocab in _CONCEPT_CLUSTERS:
                    overlap = len(token_set & vocab)
                    if overlap > 0:
                        weight = 2.4 * math.log1p(overlap)
                        for slot in range(4):
                            ic, sc = _hash_feature(f"{concept_name}:{slot}", dims)
                            vec[ic] += sc * weight

            vectors.append(_normalize(vec))

        return EmbeddingResult(vectors=vectors, model=model, prompt_tokens=total_tokens)

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_seconds: float = 18.0,
        json_schema: dict[str, Any] | None = None,
    ) -> ChatResult:
        self._maybe_fault(timeout_seconds, user_prompt)
        p_toks = max(4, (len(system_prompt.split()) + len(user_prompt.split())) * 4 // 3)

        lower_user = user_prompt.lower()
        props = set((json_schema or {}).get("properties", {}).keys())

        if "requested_tools" in props or "urgency" in props:
            text = self._synthesize_triage_json(user_prompt, lower_user)
        elif "verdict" in props and "quality_score" in props:
            text = self._synthesize_review_json(user_prompt)
        else:
            text = self._synthesize_grounded_answer(user_prompt)

        c_toks = max(4, len(text.split()) * 4 // 3)
        return ChatResult(
            text=text, model=model, prompt_tokens=p_toks, completion_tokens=c_toks
        )

    def health_check(self, timeout_seconds: float = 2.0) -> bool:
        return not self.outage_active

    @staticmethod
    def _synthesize_triage_json(user_prompt: str, lower: str) -> str:
        if any(k in lower for k in ("compromised", "leaked", "gdpr", "erasure", "ssn", "security")):
            category, urgency, sentiment = "security_privacy", "p1_critical", "urgent"
            tools = ["kb_search", "account_sla_lookup"]
        elif any(k in lower for k in ("saml", "sso", "okta", "login", "scim")):
            category, urgency, sentiment = "auth_sso", "p2_high", "frustrated"
            tools = ["kb_search", "account_sla_lookup"]
        elif any(k in lower for k in ("invoice", "vat", "refund", "billing", "charge")):
            category, urgency, sentiment = "billing_refund", "p3_normal", "neutral"
            tools = ["kb_search", "billing_ledger_check"]
        elif any(k in lower for k in ("429", "rate limit", "throttle", "backoff")):
            category, urgency, sentiment = "rate_limits", "p2_high", "frustrated"
            tools = ["kb_search", "account_sla_lookup"]
        elif any(k in lower for k in ("webhook", "hmac", "signature")):
            category, urgency, sentiment = "api_webhooks", "p2_high", "urgent"
            tools = ["kb_search"]
        else:
            category, urgency, sentiment = "general", "p3_normal", "neutral"
            tools = ["kb_search"]

        lines = [ln.strip() for ln in user_prompt.splitlines() if ln.strip()]
        query = next((ln.split(":", 1)[-1].strip() for ln in lines if ln.lower().startswith("subject:")), lines[-1][:120])
        body_line = next((ln.split(":", 1)[-1].strip() for ln in lines if ln.lower().startswith("body:")), "")
        search_q = f"{query} {body_line}".strip()[:180]
        return json.dumps(
            {
                "category": category,
                "urgency": urgency,
                "sentiment": sentiment,
                "summary": f"Customer reported {category} issue: {query[:90]}",
                "search_query": search_q,
                "requested_tools": tools,
            }
        )

    @staticmethod
    def _synthesize_review_json(user_prompt: str) -> str:
        flags: list[str] = []
        if "@" in user_prompt.split("DRAFT_REPLY:", 1)[-1]:
            flags.append("potential_unredacted_email")
        has_citation = "[" in user_prompt.split("DRAFT_REPLY:", 1)[-1]
        if not has_citation:
            flags.append("missing_kb_citation")
        verdict = "needs_revision" if flags else "ready_for_human"
        score = 0.62 if flags else 0.94
        feedback = (
            "Add explicit [KB-xxx] citation and remove any raw email addresses."
            if flags
            else "Grounded in retrieved KB articles, PII-safe, and polite."
        )
        return json.dumps(
            {
                "verdict": verdict,
                "quality_score": score,
                "policy_flags": flags,
                "feedback": feedback,
            }
        )

    @staticmethod
    def _synthesize_grounded_answer(user_prompt: str) -> str:
        citations = list(dict.fromkeys(_DOC_CITATION_RE.findall(user_prompt)))
        cite_tag = f" [{citations[0]}]" if citations else ""
        context_lines = [
            ln.strip()
            for ln in user_prompt.splitlines()
            if ln.strip() and not ln.strip().startswith(("QUESTION:", "TICKET:", "INSTRUCTIONS:"))
        ]
        key_fact = context_lines[-1][:240] if context_lines else "Please follow our support runbook."
        return (
            f"Based on our latest knowledge base{cite_tag}, {key_fact} "
            f"Let us know if you need any further assistance{cite_tag}."
        )

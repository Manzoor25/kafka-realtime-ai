"""Deterministic PII & secret redaction engine used before embedding and audit logging."""

from __future__ import annotations

import re
from dataclasses import dataclass

# Ordered from most specific to broader patterns to avoid partial masks
_API_KEY_RE = re.compile(
    r"\b(?:sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})\b"
)
_SSN_RE = re.compile(r"\b(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b")
_CC_CANDIDATE_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]\d{3}[-.\s]\d{4}(?!\d)"
)
_IPV4_RE = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"
)


@dataclass(frozen=True)
class RedactionResult:
    sanitized_text: str
    total_redactions: int
    counts_by_type: dict[str, int]


def _luhn_valid(digits: str) -> bool:
    if not (13 <= len(digits) <= 19) or len(set(digits)) == 1:
        return False
    total = 0
    reverse_digits = digits[::-1]
    for idx, ch in enumerate(reverse_digits):
        n = ord(ch) - 48
        if idx % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def redact_pii(text: str) -> RedactionResult:
    """Replaces emails, phones, SSNs, Luhn-valid payment cards, API tokens/JWTs, and IPv4s."""
    if not text:
        return RedactionResult(sanitized_text="", total_redactions=0, counts_by_type={})

    counts: dict[str, int] = {}

    def _bump(kind: str) -> str:
        counts[kind] = counts.get(kind, 0) + 1
        return f"[REDACTED_{kind.upper()}]"

    out = _API_KEY_RE.sub(lambda _m: _bump("secret"), text)
    out = _SSN_RE.sub(lambda _m: _bump("ssn"), out)

    def _replace_cc(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = "".join(ch for ch in raw if ch.isdigit())
        if _luhn_valid(digits):
            return _bump("credit_card")
        return raw

    out = _CC_CANDIDATE_RE.sub(_replace_cc, out)
    out = _EMAIL_RE.sub(lambda _m: _bump("email"), out)
    out = _PHONE_RE.sub(lambda _m: _bump("phone"), out)
    out = _IPV4_RE.sub(lambda _m: _bump("ip_address"), out)

    return RedactionResult(
        sanitized_text=out,
        total_redactions=sum(counts.values()),
        counts_by_type=counts,
    )

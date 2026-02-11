from __future__ import annotations

from datetime import UTC, datetime


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def tokenize(text: str) -> set[str]:
    punctuation = ".,!?;:\"'()[]{}"
    return {token.strip(punctuation) for token in text.lower().split() if token.strip()}

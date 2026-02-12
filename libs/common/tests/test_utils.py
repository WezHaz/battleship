from __future__ import annotations

from datetime import datetime

import pytest
from common.utils import now_utc_iso, tokenize

pytestmark = pytest.mark.unit


def test_tokenize_normalizes_case_and_punctuation() -> None:
    tokens = tokenize("Python, APIs! python;")
    assert tokens == {"python", "apis"}


def test_tokenize_returns_empty_set_for_blank_text() -> None:
    assert tokenize("   ") == set()


def test_now_utc_iso_returns_parseable_utc_timestamp() -> None:
    parsed = datetime.fromisoformat(now_utc_iso())
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() is not None
    assert parsed.utcoffset().total_seconds() == 0

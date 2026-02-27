from __future__ import annotations

import importlib.util

# Emailer tests depend on optional pydantic email extras.
# Skip collecting them when email-validator is not installed.
if importlib.util.find_spec("email_validator") is None:
    collect_ignore_glob = [
        "services/emailer/tests/*",
        "tests/bdd/test_emailer_bdd.py",
        "tests/test_smoke_harness.py",
    ]

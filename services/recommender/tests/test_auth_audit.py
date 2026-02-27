from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from recommender.main import create_app

pytestmark = pytest.mark.integration


@pytest.fixture
def client(tmp_path: Path):
    db_path = tmp_path / "recommender.sqlite3"
    app = create_app(
        database_path=str(db_path),
        api_tokens={
            "token-postings": {"postings:write"},
            "token-scan": {"scan"},
            "token-profiles": {"profiles:write"},
            "token-audit": {"audit:read"},
            "token-admin": {"*"},
        },
    )
    with TestClient(app) as test_client:
        yield test_client


def test_scope_enforcement_for_write_endpoints(client: TestClient) -> None:
    forbidden = client.post(
        "/postings",
        headers={"x-api-key": "token-scan"},
        json={
            "postings": [
                {"id": "job-1", "title": "Backend Engineer", "description": "Build Python APIs"}
            ]
        },
    )
    assert forbidden.status_code == 403

    allowed = client.post(
        "/postings",
        headers={"x-api-key": "token-postings"},
        json={
            "postings": [
                {"id": "job-1", "title": "Backend Engineer", "description": "Build Python APIs"}
            ]
        },
    )
    assert allowed.status_code == 200
    assert allowed.json() == {"updated": 1}
    assert allowed.headers.get("x-audit-event-id")


def test_audit_events_capture_status_and_action(client: TestClient) -> None:
    missing_key = client.post(
        "/job-sources/scan",
        json={},
    )
    assert missing_key.status_code == 401

    forbidden = client.post(
        "/job-sources/scan",
        headers={"x-api-key": "token-postings"},
        json={},
    )
    assert forbidden.status_code == 403

    create_source = client.post(
        "/job-sources",
        headers={"x-api-key": "token-admin"},
        json={
            "source_id": "demo",
            "name": "Demo Source",
            "source_type": "inline_json",
            "postings": [{"title": "Backend Engineer", "description": "Build Python APIs"}],
            "enabled": True,
        },
    )
    assert create_source.status_code == 200

    scan_ok = client.post(
        "/job-sources/scan",
        headers={"x-api-key": "token-scan"},
        json={},
    )
    assert scan_ok.status_code == 200

    events_response = client.get(
        "/audit-events?limit=20",
        headers={"x-api-key": "token-audit"},
    )
    assert events_response.status_code == 200
    events = events_response.json()
    assert events_response.headers.get("x-audit-event-id")
    statuses = {event["status"] for event in events}
    actions = {event["action"] for event in events}
    assert any(event.get("request_id") for event in events)
    assert "unauthorized" in statuses
    assert "forbidden" in statuses
    assert "ok" in statuses
    assert "job_source_scan_all" in actions
    assert "job_source_upsert" in actions

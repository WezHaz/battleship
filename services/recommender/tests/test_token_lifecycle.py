from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from recommender.main import create_app

pytestmark = pytest.mark.integration


@pytest.fixture
def client(tmp_path: Path):
    db_path = tmp_path / "recommender.sqlite3"
    app = create_app(database_path=str(db_path), api_key="bootstrap-key")
    with TestClient(app) as test_client:
        yield test_client


def test_hybrid_token_lifecycle_create_use_revoke(client: TestClient) -> None:
    create_response = client.post(
        "/auth/tokens",
        headers={"x-api-key": "bootstrap-key"},
        json={
            "name": "posting-agent",
            "scopes": ["postings:write"],
            "expires_in_days": 30,
            "notes": "agent token for posting updates",
        },
    )
    assert create_response.status_code == 200
    assert create_response.headers.get("x-audit-event-id")
    create_body = create_response.json()
    issued_token = create_body["token"]
    token_id = create_body["metadata"]["token_id"]
    assert issued_token.startswith("obs_")

    postings_response = client.post(
        "/postings",
        headers={"x-api-key": issued_token},
        json={
            "postings": [
                {"id": "job-1", "title": "Backend Engineer", "description": "Build Python APIs"}
            ]
        },
    )
    assert postings_response.status_code == 200
    assert postings_response.json() == {"updated": 1}

    list_with_agent_token = client.get("/auth/tokens", headers={"x-api-key": issued_token})
    assert list_with_agent_token.status_code == 403

    list_with_bootstrap = client.get("/auth/tokens", headers={"x-api-key": "bootstrap-key"})
    assert list_with_bootstrap.status_code == 200
    tokens = list_with_bootstrap.json()
    assert any(token["token_id"] == token_id for token in tokens)

    revoke_response = client.post(
        f"/auth/tokens/{token_id}/revoke",
        headers={"x-api-key": "bootstrap-key"},
    )
    assert revoke_response.status_code == 200
    assert revoke_response.json() == {"revoked": True}

    rejected_after_revoke = client.post(
        "/postings",
        headers={"x-api-key": issued_token},
        json={
            "postings": [
                {"id": "job-2", "title": "Data Engineer", "description": "Build ETL pipelines"}
            ]
        },
    )
    assert rejected_after_revoke.status_code == 401

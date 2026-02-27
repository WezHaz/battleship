from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from recommender.main import create_app

pytestmark = pytest.mark.integration


@pytest.fixture
def client(tmp_path: Path):
    db_path = tmp_path / "recommender.sqlite3"
    app = create_app(database_path=str(db_path))
    with TestClient(app) as test_client:
        yield test_client


def test_request_id_header_and_metrics_snapshot(client: TestClient) -> None:
    first = client.get("/health")
    second = client.get("/health")
    not_found = client.get("/missing-endpoint")
    metrics = client.get("/metrics")

    assert first.status_code == 200
    assert second.status_code == 200
    assert not_found.status_code == 404
    assert metrics.status_code == 200

    first_request_id = first.headers.get("x-request-id")
    second_request_id = second.headers.get("x-request-id")
    metrics_request_id = metrics.headers.get("x-request-id")
    assert first_request_id
    assert second_request_id
    assert metrics_request_id
    assert first_request_id != second_request_id

    body = metrics.json()
    assert body["totals"]["requests"] >= 3
    assert body["totals"]["errors"] >= 1
    assert "GET /health" in body["endpoints"]
    assert body["endpoints"]["GET /health"]["count"] >= 2


def test_incoming_request_id_is_preserved(client: TestClient) -> None:
    response = client.get("/health", headers={"x-request-id": "manual-request-id"})
    assert response.status_code == 200
    assert response.headers.get("x-request-id") == "manual-request-id"

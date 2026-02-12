import pytest
from fastapi.testclient import TestClient
from recommender.main import app

pytestmark = pytest.mark.integration


def test_health() -> None:
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "recommender"}

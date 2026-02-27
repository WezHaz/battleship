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


def test_can_store_postings_and_recommend_using_persisted_data(client: TestClient) -> None:
    store_response = client.post(
        "/postings",
        json={
            "postings": [
                {
                    "id": "job-1",
                    "title": "Backend Engineer",
                    "description": "Build Python APIs and maintain CI tooling",
                },
                {
                    "id": "job-2",
                    "title": "Data Scientist",
                    "description": "Train and deploy machine learning models",
                },
            ]
        },
    )
    assert store_response.status_code == 200
    assert store_response.json() == {"updated": 2}

    recommend_response = client.post(
        "/recommend",
        json={
            "resume_text": "Backend engineer focused on Python APIs and platform tooling.",
            "postings": [],
        },
    )
    assert recommend_response.status_code == 200
    body = recommend_response.json()
    assert body["source"] == "stored"
    assert body["recommendations"][0]["id"] == "job-1"
    assert body["run_id"] >= 1


def test_recommendation_history_tracks_runs(client: TestClient) -> None:
    client.post(
        "/recommend",
        json={
            "resume_text": "Backend engineer focused on Python APIs and platform tooling.",
            "postings": [
                {
                    "id": "job-1",
                    "title": "Backend Engineer",
                    "description": "Build Python APIs and maintain CI tooling",
                }
            ],
        },
    )

    history_response = client.get("/recommendations/history")
    assert history_response.status_code == 200
    runs = history_response.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["recommendation_count"] == 1

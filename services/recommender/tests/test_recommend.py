from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from recommender.main import app

pytestmark = pytest.mark.integration


def test_recommend_ranks_postings_by_token_overlap() -> None:
    payload = {
        "resume_text": "Experienced backend python developer building API systems and tooling.",
        "postings": [
            {
                "id": "job-1",
                "title": "Backend Engineer",
                "description": "Build Python API services",
            },
            {
                "id": "job-2",
                "title": "Data Scientist",
                "description": "Train machine learning models",
            },
            {
                "id": "job-3",
                "title": "Platform Engineer",
                "description": "Own developer tooling and CI pipelines",
            },
        ],
    }

    with TestClient(app) as client:
        response = client.post("/recommend", json=payload)

    body = response.json()
    assert response.status_code == 200
    assert "generated_at" in body
    assert [item["id"] for item in body["recommendations"]] == ["job-1", "job-3", "job-2"]
    assert body["recommendations"][0]["score"] >= body["recommendations"][1]["score"]


def test_recommend_rejects_resume_shorter_than_minimum_length() -> None:
    payload = {"resume_text": "too short", "postings": []}

    with TestClient(app) as client:
        response = client.post("/recommend", json=payload)

    assert response.status_code == 422

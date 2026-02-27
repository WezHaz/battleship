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


def test_profile_can_drive_recommendation_preferences(client: TestClient) -> None:
    create_response = client.post(
        "/profiles",
        json={
            "profile_id": "wesley_remote",
            "name": "Wesley Remote",
            "preferred_keywords": ["python", "api", "backend"],
            "preferred_locations": ["Remote"],
            "preferred_companies": ["Acme Labs"],
            "remote_only": True,
        },
    )
    assert create_response.status_code == 200

    recommend_response = client.post(
        "/recommend",
        json={
            "resume_text": "Backend engineer building Python API services and tooling.",
            "profile_id": "wesley_remote",
            "postings": [
                {
                    "id": "job-1",
                    "title": "Backend Engineer",
                    "description": "Build Python API services",
                    "company": "Acme Labs",
                    "location": "Remote",
                },
                {
                    "id": "job-2",
                    "title": "Backend Engineer",
                    "description": "Build Java services",
                    "company": "Other Corp",
                    "location": "Onsite",
                },
            ],
        },
    )
    assert recommend_response.status_code == 200
    body = recommend_response.json()
    assert body["applied_profile_id"] == "wesley_remote"
    assert body["recommendations"][0]["id"] == "job-1"


def test_recommend_request_overrides_profile_values(client: TestClient) -> None:
    create_response = client.post(
        "/profiles",
        json={
            "profile_id": "company_pref",
            "name": "Company Pref",
            "preferred_companies": ["Acme Labs"],
            "remote_only": True,
        },
    )
    assert create_response.status_code == 200

    recommend_response = client.post(
        "/recommend",
        json={
            "resume_text": "Backend engineer building Python API services.",
            "profile_id": "company_pref",
            "preferred_companies": ["Other Corp"],
            "remote_only": False,
            "postings": [
                {
                    "id": "job-1",
                    "title": "Backend Engineer",
                    "description": "Build Python API services",
                    "company": "Acme Labs",
                    "location": "Remote",
                },
                {
                    "id": "job-2",
                    "title": "Backend Engineer",
                    "description": "Build Python API services",
                    "company": "Other Corp",
                    "location": "Onsite",
                },
            ],
        },
    )
    assert recommend_response.status_code == 200
    body = recommend_response.json()
    assert body["recommendations"][0]["id"] == "job-2"


def test_profile_crud_and_api_key_enforcement(tmp_path: Path) -> None:
    db_path = tmp_path / "secure.sqlite3"
    app = create_app(database_path=str(db_path), api_key="secret-key")
    with TestClient(app) as client:
        denied = client.post(
            "/profiles",
            json={"profile_id": "profile_unauth", "name": "X", "remote_only": False},
        )
        assert denied.status_code == 401

        allowed = client.post(
            "/profiles",
            headers={"x-api-key": "secret-key"},
            json={"profile_id": "profile_1", "name": "Profile 1", "remote_only": False},
        )
        assert allowed.status_code == 200

        listed = client.get("/profiles")
        assert listed.status_code == 200
        assert len(listed.json()) == 1

        deleted = client.delete("/profiles/profile_1", headers={"x-api-key": "secret-key"})
        assert deleted.status_code == 200
        assert deleted.json() == {"deleted": True}

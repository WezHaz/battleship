from __future__ import annotations

import json
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


def test_inline_job_source_scan_persists_postings(client: TestClient) -> None:
    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "builtin_demo",
            "name": "Builtin Demo",
            "source_type": "inline_json",
            "postings": [
                {"title": "Backend Engineer", "description": "Build Python APIs"},
                {"title": "ML Engineer", "description": "Train and deploy ML models"},
            ],
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    scan_response = client.post("/job-sources/scan")
    assert scan_response.status_code == 200
    scan_body = scan_response.json()
    assert scan_body["requested_sources"] == 1
    assert scan_body["successful_sources"] == 1
    assert scan_body["total_ingested"] == 2

    recommend_response = client.post(
        "/recommend",
        json={
            "resume_text": "Python backend engineer building APIs and services.",
            "postings": [],
        },
    )
    assert recommend_response.status_code == 200
    body = recommend_response.json()
    assert body["source"] == "stored"
    assert body["recommendations"][0]["title"] == "Backend Engineer"


def test_json_url_source_scan_fetches_remote_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"postings": [{"title": "Platform Engineer", "description": "Own CI tooling"}]}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *_: object) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(payload).encode("utf-8")

    def fake_urlopen(url: str, timeout: int = 15) -> FakeResponse:
        del timeout
        assert url == "https://example.com/postings.json"
        return FakeResponse()

    monkeypatch.setattr("recommender.main.urllib_request.urlopen", fake_urlopen)

    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "remote_demo",
            "name": "Remote Demo",
            "source_type": "json_url",
            "url": "https://example.com/postings.json",
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    scan_response = client.post("/job-sources/remote_demo/scan")
    assert scan_response.status_code == 200
    body = scan_response.json()
    assert body["status"] == "ok"
    assert body["ingested"] == 1


def test_light_dedup_keeps_duplicates_without_external_ids(client: TestClient) -> None:
    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "dupe_demo",
            "name": "Duplicate Demo",
            "source_type": "inline_json",
            "postings": [
                {
                    "title": "Backend Engineer",
                    "description": "Build Python APIs",
                    "company": "Example Corp",
                    "location": "Remote",
                }
            ],
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    first_scan = client.post("/job-sources/dupe_demo/scan")
    second_scan = client.post("/job-sources/dupe_demo/scan")
    assert first_scan.status_code == 200
    assert second_scan.status_code == 200

    postings_response = client.get("/postings?limit=10")
    assert postings_response.status_code == 200
    postings = [item for item in postings_response.json() if item["source_id"] == "dupe_demo"]
    assert len(postings) == 2
    assert postings[0]["dedup_key"] == postings[1]["dedup_key"]
    assert any(item["duplicate_hint_count"] >= 1 for item in postings)


def test_external_id_dedup_updates_single_record(client: TestClient) -> None:
    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "stable_external",
            "name": "Stable External IDs",
            "source_type": "inline_json",
            "postings": [
                {
                    "external_id": "abc-1",
                    "title": "Backend Engineer",
                    "description": "Build APIs v1",
                }
            ],
            "enabled": True,
        },
    )
    assert source_response.status_code == 200
    assert client.post("/job-sources/stable_external/scan").status_code == 200

    update_source = client.post(
        "/job-sources",
        json={
            "source_id": "stable_external",
            "name": "Stable External IDs",
            "source_type": "inline_json",
            "postings": [
                {
                    "external_id": "abc-1",
                    "title": "Backend Engineer",
                    "description": "Build APIs v2",
                }
            ],
            "enabled": True,
        },
    )
    assert update_source.status_code == 200
    assert client.post("/job-sources/stable_external/scan").status_code == 200

    postings_response = client.get("/postings?limit=20")
    postings = [item for item in postings_response.json() if item["source_id"] == "stable_external"]
    assert len(postings) == 1
    assert postings[0]["description"] == "Build APIs v2"


def test_write_endpoints_require_api_key_when_configured(tmp_path: Path) -> None:
    db_path = tmp_path / "secured.sqlite3"
    app = create_app(database_path=str(db_path), api_key="secret-key")
    with TestClient(app) as client:
        denied = client.post(
            "/postings",
            json={
                "postings": [
                    {"id": "job-1", "title": "Backend Engineer", "description": "Build Python APIs"}
                ]
            },
        )
        assert denied.status_code == 401

        allowed = client.post(
            "/postings",
            headers={"x-api-key": "secret-key"},
            json={
                "postings": [
                    {"id": "job-1", "title": "Backend Engineer", "description": "Build Python APIs"}
                ]
            },
        )
        assert allowed.status_code == 200
        assert allowed.json() == {"updated": 1}

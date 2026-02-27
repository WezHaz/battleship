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


def test_scan_backoff_skip_and_history(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingResponse:
        def __enter__(self) -> FailingResponse:
            return self

        def __exit__(self, *_: object) -> bool:
            return False

        def read(self) -> bytes:
            raise RuntimeError("upstream unavailable")

    def failing_urlopen(url: str, timeout: int = 15) -> FailingResponse:
        del timeout
        assert url == "https://example.com/failing.json"
        return FailingResponse()

    monkeypatch.setattr("recommender.main.urllib_request.urlopen", failing_urlopen)

    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "failing_demo",
            "name": "Failing Demo",
            "source_type": "json_url",
            "url": "https://example.com/failing.json",
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    first_scan = client.post("/job-sources/failing_demo/scan")
    assert first_scan.status_code == 502

    skipped_scan = client.post("/job-sources/failing_demo/scan?respect_backoff=true")
    assert skipped_scan.status_code == 200
    skipped_body = skipped_scan.json()
    assert skipped_body["status"] == "skipped"
    assert skipped_body["backoff_seconds"] > 0
    assert skipped_body["attempt_number"] >= 2

    sources_response = client.get("/job-sources")
    assert sources_response.status_code == 200
    sources = {item["source_id"]: item for item in sources_response.json()}
    failing_source = sources["failing_demo"]
    assert failing_source["last_status"] == "skipped"
    assert failing_source["consecutive_failures"] == 1
    assert failing_source["next_eligible_scan_at"]
    assert failing_source["last_error"] == "upstream unavailable"

    history_response = client.get("/job-sources/scan-history?source_id=failing_demo")
    assert history_response.status_code == 200
    history = history_response.json()
    assert len(history) >= 2
    assert history[0]["status"] == "skipped"
    assert history[0]["trigger"] == "manual"
    assert history[1]["status"] == "error"


def test_scheduled_scan_endpoint_uses_scheduled_trigger(client: TestClient) -> None:
    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "scheduled_demo",
            "name": "Scheduled Demo",
            "source_type": "inline_json",
            "postings": [{"title": "Data Engineer", "description": "Build data products"}],
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    scan_response = client.post("/job-sources/scan/scheduled")
    assert scan_response.status_code == 200
    body = scan_response.json()
    assert body["trigger"] == "scheduled"
    assert body["respect_backoff"] is True
    assert body["requested_sources"] == 1
    assert body["successful_sources"] == 1
    assert body["results"][0]["trigger"] == "scheduled"

    history_response = client.get("/job-sources/scan-history?trigger=scheduled")
    assert history_response.status_code == 200
    history = history_response.json()
    assert len(history) >= 1
    assert history[0]["trigger"] == "scheduled"


def test_scan_history_supports_status_date_and_pagination(client: TestClient) -> None:
    source_response = client.post(
        "/job-sources",
        json={
            "source_id": "history_filter_demo",
            "name": "History Filter Demo",
            "source_type": "inline_json",
            "postings": [{"title": "Platform Engineer", "description": "Build platform APIs"}],
            "enabled": True,
        },
    )
    assert source_response.status_code == 200

    first_scan = client.post("/job-sources/history_filter_demo/scan")
    second_scan = client.post("/job-sources/history_filter_demo/scan")
    assert first_scan.status_code == 200
    assert second_scan.status_code == 200

    after_ts = "2020-01-01T00:00:00Z"
    before_ts = "2099-01-01T00:00:00Z"
    history_first_page = client.get(
        "/job-sources/scan-history"
        "?limit=1&offset=0&source_id=history_filter_demo&trigger=manual"
        f"&status=ok&scanned_after={after_ts}&scanned_before={before_ts}"
    )
    assert history_first_page.status_code == 200
    page_one = history_first_page.json()
    assert len(page_one) == 1
    assert page_one[0]["source_id"] == "history_filter_demo"
    assert page_one[0]["status"] == "ok"

    history_second_page = client.get(
        "/job-sources/scan-history?limit=1&offset=1&source_id=history_filter_demo&status=ok"
    )
    assert history_second_page.status_code == 200
    page_two = history_second_page.json()
    assert len(page_two) == 1
    assert page_two[0]["history_id"] != page_one[0]["history_id"]


def test_scan_history_rejects_invalid_timestamps(client: TestClient) -> None:
    response = client.get("/job-sources/scan-history?scanned_after=not-a-timestamp")
    assert response.status_code == 422

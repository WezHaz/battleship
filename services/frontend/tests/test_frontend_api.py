from __future__ import annotations

from typing import Any

import frontend.main as frontend_main
import httpx
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


class StubResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._payload


class StubAsyncClient:
    def __init__(self, response: StubResponse, capture: dict[str, Any]) -> None:
        self.response = response
        self.capture = capture

    async def __aenter__(self) -> StubAsyncClient:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> StubResponse:
        self.capture["method"] = method
        self.capture["url"] = url
        self.capture["json"] = json
        self.capture["headers"] = headers or {}
        return self.response


class ErroringAsyncClient:
    async def __aenter__(self) -> ErroringAsyncClient:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> StubResponse:
        del method, json, headers
        request = httpx.Request("POST", url)
        raise httpx.RequestError("connection failed", request=request)


def test_health() -> None:
    with TestClient(frontend_main.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "frontend"}


def test_proxy_recommend_wraps_upstream_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {
        "generated_at": "2026-02-11T10:00:00+00:00",
        "recommendations": [{"id": "job-1", "title": "Backend Engineer", "score": 0.75}],
    }
    response = StubResponse(
        status_code=200,
        payload=upstream_payload,
        headers={"x-request-id": "req-123", "x-audit-event-id": "71"},
    )
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post(
            "/api/recommend",
            json={
                "resume_text": "Experienced backend python engineer building API services.",
                "postings": ["Backend Engineer", "ML Engineer"],
                "profile_id": "wesley_remote",
                "preferred_keywords": [],
                "preferred_locations": [],
                "preferred_companies": [],
                "remote_only": None,
            },
        )

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert "gateway_generated_at" in body
    assert body["upstream_request_id"] == "req-123"
    assert body["upstream_audit_event_id"] == "71"
    assert body["recommender_response"] == upstream_payload
    assert capture["method"] == "POST"
    assert capture["url"].endswith("/recommend")
    assert capture["json"]["profile_id"] == "wesley_remote"
    assert [posting["id"] for posting in capture["json"]["postings"]] == ["job-1", "job-2"]


def test_proxy_profile_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {"profile_id": "wesley_remote", "name": "Wesley Remote"}
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        upsert = client.post(
            "/api/profiles",
            json={
                "profile_id": "wesley_remote",
                "name": "Wesley Remote",
                "preferred_keywords": ["python"],
                "preferred_locations": ["remote"],
                "preferred_companies": ["acme"],
                "remote_only": True,
            },
        )
        listed = client.get("/api/profiles")
        deleted = client.delete("/api/profiles/wesley_remote")

    assert upsert.status_code == 200
    assert listed.status_code == 200
    assert deleted.status_code == 200
    assert capture["url"].endswith("/profiles/wesley_remote")
    assert capture["method"] == "DELETE"


def test_proxy_list_sources_defaults_to_all_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = [{"source_id": "demo", "enabled": True}]
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.get("/api/sources")

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert body["recommender_response"] == upstream_payload
    assert capture["method"] == "GET"
    assert capture["url"].endswith("/job-sources?enabled_only=false")


def test_proxy_scan_one_source(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {"source_id": "demo", "status": "ok", "ingested": 2}
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post("/api/scan/sources/demo")

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert body["recommender_response"] == upstream_payload
    assert capture["method"] == "POST"
    assert capture["url"].endswith("/job-sources/demo/scan?respect_backoff=false")


def test_proxy_scan_wraps_upstream_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {"updated": 2}
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post(
            "/api/scan",
            json={"postings": ["Backend Engineer", "ML Engineer"]},
        )

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert "gateway_generated_at" in body
    assert body["recommender_response"] == upstream_payload
    assert capture["method"] == "POST"
    assert capture["url"].endswith("/postings")
    assert [posting["id"] for posting in capture["json"]["postings"]] == ["job-1", "job-2"]


def test_proxy_scan_rejects_empty_postings() -> None:
    with TestClient(frontend_main.app) as client:
        response = client.post("/api/scan", json={"postings": []})

    assert response.status_code == 422


def test_proxy_scan_sources_wraps_upstream_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {"requested_sources": 1, "successful_sources": 1, "results": []}
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post("/api/scan/sources", json={"enabled_only": True})

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert "gateway_generated_at" in body
    assert body["recommender_response"] == upstream_payload
    assert capture["url"].endswith("/job-sources/scan?enabled_only=true&respect_backoff=false")


def test_proxy_scan_sources_scheduled_path(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {"requested_sources": 1, "successful_sources": 1, "results": []}
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post(
            "/api/scan/sources",
            json={"enabled_only": False, "trigger": "scheduled", "respect_backoff": False},
        )

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert body["recommender_response"] == upstream_payload
    assert capture["url"].endswith("/job-sources/scan/scheduled?enabled_only=false")


def test_proxy_scan_history(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = [{"source_id": "demo", "status": "ok"}]
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.get(
            "/api/scan/history"
            "?limit=10&offset=5&source_id=demo&trigger=manual&status=ok"
            "&scanned_after=2026-02-01T00:00:00Z&scanned_before=2026-02-27T00:00:00Z"
        )

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert body["recommender_response"] == upstream_payload
    assert capture["method"] == "GET"
    assert "/job-sources/scan-history?" in capture["url"]
    assert "limit=10" in capture["url"]
    assert "offset=5" in capture["url"]
    assert "source_id=demo" in capture["url"]
    assert "trigger=manual" in capture["url"]
    assert "status=ok" in capture["url"]
    assert "scanned_after=" in capture["url"]
    assert "scanned_before=" in capture["url"]


def test_proxy_scan_sources_maps_upstream_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    error_response = StubResponse(status_code=500, payload={"detail": "failure"})
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=error_response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        response = client.post("/api/scan/sources", json={"enabled_only": True})

    assert response.status_code == 502
    assert response.json() == {"detail": "failure"}


def test_proxy_recommend_maps_upstream_client_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    error_response = StubResponse(status_code=404, payload={"detail": "Unknown profile_id"})
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=error_response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        response = client.post(
            "/api/recommend",
            json={
                "resume_text": "Experienced backend python engineer building API services.",
                "postings": [],
                "profile_id": "missing",
            },
        )

    assert response.status_code == 404
    assert response.json() == {"detail": "Unknown profile_id"}


def test_proxy_recommend_maps_upstream_connectivity_errors_to_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: ErroringAsyncClient(),
    )

    with TestClient(frontend_main.app) as client:
        response = client.post(
            "/api/recommend",
            json={
                "resume_text": "Experienced backend python engineer building API services.",
                "postings": ["Backend Engineer"],
            },
        )

    assert response.status_code == 502
    assert response.json() == {"detail": "Upstream recommender is unavailable"}


def test_proxy_recommend_forwards_api_key_header(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    upstream_payload = {
        "generated_at": "2026-02-11T10:00:00+00:00",
        "recommendations": [],
    }
    response = StubResponse(status_code=200, payload=upstream_payload)
    monkeypatch.setattr(frontend_main, "RECOMMENDER_API_KEY", "test-key")
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        proxy_response = client.post(
            "/api/recommend",
            json={
                "resume_text": "Experienced backend python engineer building API services.",
                "postings": ["Backend Engineer"],
            },
        )

    assert proxy_response.status_code == 200
    assert capture["headers"] == {"x-api-key": "test-key"}

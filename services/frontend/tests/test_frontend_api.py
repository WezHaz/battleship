from __future__ import annotations

from typing import Any

import frontend.main as frontend_main
import httpx
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


class StubResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

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

    async def post(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> StubResponse:
        self.capture["url"] = url
        self.capture["json"] = json
        self.capture["headers"] = headers or {}
        return self.response


class ErroringAsyncClient:
    async def __aenter__(self) -> ErroringAsyncClient:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def post(
        self,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> StubResponse:
        del json, headers
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
    response = StubResponse(status_code=200, payload=upstream_payload)
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
            },
        )

    body = proxy_response.json()
    assert proxy_response.status_code == 200
    assert "gateway_generated_at" in body
    assert body["recommender_response"] == upstream_payload
    assert capture["url"].endswith("/recommend")
    assert [posting["id"] for posting in capture["json"]["postings"]] == ["job-1", "job-2"]


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
    assert capture["url"].endswith("/job-sources/scan?enabled_only=true")


def test_proxy_scan_sources_maps_upstream_errors_to_502(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert response.json() == {"detail": "Upstream recommender request failed"}


def test_proxy_recommend_maps_upstream_errors_to_502(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    error_response = StubResponse(status_code=500, payload={"detail": "failure"})
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
                "postings": ["Backend Engineer"],
            },
        )

    assert response.status_code == 502
    assert response.json() == {"detail": "Upstream recommender request failed"}


def test_proxy_scan_maps_upstream_errors_to_502(monkeypatch: pytest.MonkeyPatch) -> None:
    capture: dict[str, Any] = {}
    error_response = StubResponse(status_code=500, payload={"detail": "failure"})
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=error_response, capture=capture),
    )

    with TestClient(frontend_main.app) as client:
        response = client.post("/api/scan", json={"postings": ["Backend Engineer"]})

    assert response.status_code == 502
    assert response.json() == {"detail": "Upstream recommender request failed"}


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

from __future__ import annotations

import asyncio
from typing import Any

import emailer.main as emailer_main
import frontend.main as frontend_main
import pytest
import recommender.main as recommender_main
from fastapi.testclient import TestClient

pytestmark = [pytest.mark.integration, pytest.mark.smoke]


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
    def __init__(self, response: StubResponse) -> None:
        self.response = response

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
        del method, url, json, headers
        return self.response


class FakeWorker:
    def __init__(self) -> None:
        self.queued_jobs = 0

    async def run(self) -> None:
        await asyncio.Event().wait()

    async def enqueue(self, job: emailer_main.DigestJob) -> int:
        del job
        self.queued_jobs += 1
        return self.queued_jobs


def test_smoke_recommender_ready_and_ranking() -> None:
    payload = {
        "resume_text": "Experienced backend python engineer building APIs and automation systems.",
        "postings": [
            {
                "id": "job-1",
                "title": "Backend Engineer",
                "description": "Build Python APIs and services",
            }
        ],
    }

    with TestClient(recommender_main.app) as client:
        health = client.get("/health")
        response = client.post("/recommend", json=payload)

    assert health.status_code == 200
    assert response.status_code == 200
    body = response.json()
    assert len(body["recommendations"]) == 1
    assert body["recommendations"][0]["id"] == "job-1"


def test_smoke_frontend_proxy_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    upstream_payload = {
        "generated_at": "2026-02-12T10:00:00+00:00",
        "recommendations": [{"id": "job-1", "title": "Backend Engineer", "score": 0.9}],
    }
    monkeypatch.setattr(
        frontend_main.httpx,
        "AsyncClient",
        lambda *_, **__: StubAsyncClient(response=StubResponse(200, upstream_payload)),
    )

    with TestClient(frontend_main.app) as client:
        health = client.get("/health")
        response = client.post(
            "/api/recommend",
            json={
                "resume_text": "Experienced backend python engineer building API services.",
                "postings": ["Backend Engineer"],
            },
        )

    assert health.status_code == 200
    assert response.status_code == 200
    body = response.json()
    assert "gateway_generated_at" in body
    assert body["recommender_response"] == upstream_payload


def test_smoke_emailer_digest_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_worker = FakeWorker()
    monkeypatch.setattr(emailer_main, "worker", fake_worker)

    with TestClient(emailer_main.app) as client:
        health = client.get("/health")
        response = client.post(
            "/cron/digest",
            json={
                "recipients": ["one@example.com"],
                "jobs": ["Backend Engineer"],
            },
        )

    assert health.status_code == 200
    assert response.status_code == 200
    assert response.json()["queued_jobs"] == 1

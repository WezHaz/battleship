from __future__ import annotations

from typing import Any

import frontend.main as frontend_main
import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, scenario, then, when

pytestmark = pytest.mark.bdd


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


@scenario("features/frontend.feature", "Proxy recommendation requests to recommender")
def test_frontend_proxy() -> None:
    pass


@pytest.fixture
def context() -> dict[str, object]:
    return {}


@given("a frontend request payload")
def given_frontend_request_payload(context: dict[str, object]) -> None:
    context["payload"] = {
        "resume_text": "Experienced backend python engineer building API services.",
        "postings": ["Backend Engineer", "ML Engineer"],
    }


@given("the recommender upstream succeeds")
def given_recommender_upstream_success(
    context: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
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
    context["capture"] = capture
    context["upstream_payload"] = upstream_payload


@when("the frontend proxy endpoint is called", target_fixture="response")
def when_frontend_proxy_is_called(context: dict[str, object]):
    with TestClient(frontend_main.app) as client:
        return client.post("/api/recommend", json=context["payload"])


@then("the frontend response is successful")
def then_frontend_response_is_successful(response) -> None:
    assert response.status_code == 200


@then("the frontend response includes the recommender payload")
def then_frontend_response_includes_recommender_payload(
    context: dict[str, object], response
) -> None:
    assert response.json()["recommender_response"] == context["upstream_payload"]


@then("the forwarded payload contains generated posting ids")
def then_forwarded_payload_contains_generated_ids(context: dict[str, object]) -> None:
    capture = context["capture"]
    posting_ids = [posting["id"] for posting in capture["json"]["postings"]]
    assert posting_ids == ["job-1", "job-2"]

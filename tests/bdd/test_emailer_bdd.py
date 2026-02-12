from __future__ import annotations

import asyncio

import emailer.main as emailer_main
import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, scenario, then, when

pytestmark = pytest.mark.bdd


class FakeWorker:
    def __init__(self) -> None:
        self.jobs: list[emailer_main.DigestJob] = []

    async def run(self) -> None:
        await asyncio.Event().wait()

    async def enqueue(self, job: emailer_main.DigestJob) -> int:
        self.jobs.append(job)
        return len(self.jobs)


@scenario("features/emailer.feature", "Queue a digest job for each recipient")
def test_queue_digest_job_for_each_recipient() -> None:
    pass


@pytest.fixture
def context() -> dict[str, object]:
    return {}


@given("two digest recipients and jobs")
def given_digest_request_payload(
    context: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_worker = FakeWorker()
    monkeypatch.setattr(emailer_main, "worker", fake_worker)
    context["worker"] = fake_worker
    context["payload"] = {
        "recipients": ["one@example.com", "two@example.com"],
        "jobs": ["Backend Engineer", "Platform Engineer"],
    }


@when("the digest cron endpoint is called", target_fixture="response")
def when_digest_cron_is_called(context: dict[str, object]):
    with TestClient(emailer_main.app) as client:
        return client.post("/cron/digest", json=context["payload"])


@then("the digest endpoint responds with queued status")
def then_digest_endpoint_reports_success(response) -> None:
    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["queued_jobs"] == 2


@then("two digest jobs are queued")
def then_two_digest_jobs_are_queued(context: dict[str, object]) -> None:
    fake_worker = context["worker"]
    recipients = [job.recipient for job in fake_worker.jobs]
    assert recipients == ["one@example.com", "two@example.com"]

from __future__ import annotations

import asyncio

import emailer.main as emailer_main
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


class FakeWorker:
    def __init__(self) -> None:
        self.jobs: list[emailer_main.DigestJob] = []

    async def run(self) -> None:
        await asyncio.Event().wait()

    async def enqueue(self, job: emailer_main.DigestJob) -> int:
        self.jobs.append(job)
        return len(self.jobs)


def test_health() -> None:
    with TestClient(emailer_main.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "emailer"}


def test_trigger_digest_queues_one_job_per_recipient(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_worker = FakeWorker()
    monkeypatch.setattr(emailer_main, "worker", fake_worker)

    with TestClient(emailer_main.app) as client:
        response = client.post(
            "/cron/digest",
            json={
                "recipients": ["one@example.com", "two@example.com"],
                "jobs": ["Backend Engineer", "Platform Engineer"],
            },
        )

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "queued"
    assert body["queued_jobs"] == 2
    assert [job.recipient for job in fake_worker.jobs] == ["one@example.com", "two@example.com"]

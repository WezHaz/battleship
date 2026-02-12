from __future__ import annotations

import asyncio
import contextlib

import pytest
from emailer.worker import DigestJob, DigestWorker

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_enqueue_returns_incrementing_queue_size() -> None:
    worker = DigestWorker()

    size_1 = await worker.enqueue(DigestJob(recipient="one@example.com", jobs=["Backend Engineer"]))
    size_2 = await worker.enqueue(DigestJob(recipient="two@example.com", jobs=["ML Engineer"]))

    assert size_1 == 1
    assert size_2 == 2


@pytest.mark.asyncio
async def test_run_processes_and_marks_jobs_done(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = DigestWorker()

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("emailer.worker.asyncio.sleep", no_sleep)
    task = asyncio.create_task(worker.run())

    await worker.enqueue(DigestJob(recipient="one@example.com", jobs=["Backend Engineer"]))
    await asyncio.wait_for(worker.queue.join(), timeout=1.0)

    assert worker.queue.qsize() == 0

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

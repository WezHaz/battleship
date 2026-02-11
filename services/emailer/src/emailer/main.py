from __future__ import annotations

import asyncio
import contextlib

from common.utils import now_utc_iso
from fastapi import FastAPI
from pydantic import BaseModel, EmailStr, Field

from emailer.worker import DigestJob, DigestWorker

app = FastAPI(title="OperationBattleship Emailer", version="0.1.0")
worker = DigestWorker()
worker_task: asyncio.Task | None = None


class DigestRequest(BaseModel):
    recipients: list[EmailStr] = Field(default_factory=list)
    jobs: list[str] = Field(default_factory=list)


@app.on_event("startup")
async def startup() -> None:
    global worker_task
    worker_task = asyncio.create_task(worker.run())


@app.on_event("shutdown")
async def shutdown() -> None:
    if worker_task:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "emailer"}


@app.post("/cron/digest")
async def trigger_digest(payload: DigestRequest) -> dict[str, str | int]:
    queued = 0
    for recipient in payload.recipients:
        queued = await worker.enqueue(DigestJob(recipient=str(recipient), jobs=payload.jobs))

    return {
        "status": "queued",
        "queued_jobs": queued,
        "scheduled_at": now_utc_iso(),
    }

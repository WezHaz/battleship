from __future__ import annotations

import asyncio
from dataclasses import dataclass

from common.utils import now_utc_iso


@dataclass
class DigestJob:
    recipient: str
    jobs: list[str]


class DigestWorker:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[DigestJob] = asyncio.Queue()

    async def enqueue(self, job: DigestJob) -> int:
        await self.queue.put(job)
        return self.queue.qsize()

    async def run(self) -> None:
        while True:
            job = await self.queue.get()
            try:
                # Placeholder for integration with SES/SendGrid/Mailgun.
                print(
                    f"[{now_utc_iso()}] sending digest to {job.recipient} with {len(job.jobs)} jobs"
                )
                await asyncio.sleep(0.05)
            finally:
                self.queue.task_done()

from __future__ import annotations

import os
import sqlite3
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from common.utils import now_utc_iso, tokenize
from fastapi import FastAPI, Query, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

DEFAULT_DB_PATH = os.path.join(tempfile.gettempdir(), "operation-battleship", "recommender.sqlite3")


class JobPosting(BaseModel):
    id: str = Field(..., description="Unique job identifier")
    title: str
    description: str


class RecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[JobPosting] = Field(default_factory=list)
    max_postings: int = Field(default=100, ge=1, le=500)


class RankedRecommendation(BaseModel):
    id: str
    title: str
    score: float


class RecommendResponse(BaseModel):
    run_id: int
    source: Literal["payload", "stored"]
    generated_at: str
    recommendations: list[RankedRecommendation]


class UpsertPostingsRequest(BaseModel):
    postings: list[JobPosting] = Field(default_factory=list)


class UpsertPostingsResponse(BaseModel):
    updated: int


class StoredPosting(BaseModel):
    id: str
    title: str
    description: str
    updated_at: str


class RecommendationRun(BaseModel):
    run_id: int
    generated_at: str
    recommendation_count: int


class RecommendationHistoryResponse(BaseModel):
    runs: list[RecommendationRun]


class RecommenderRepository:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)
        self._connection: sqlite3.Connection | None = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not initialized")
        return self._connection

    def connect(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS job_postings (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recommendation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resume_text TEXT NOT NULL,
                generated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recommendation_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES recommendation_runs(id) ON DELETE CASCADE,
                job_id TEXT NOT NULL,
                title TEXT NOT NULL,
                score REAL NOT NULL,
                rank INTEGER NOT NULL
            );
            """
        )
        self._connection.commit()

    def close(self) -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None

    def upsert_postings(self, postings: list[JobPosting]) -> int:
        if not postings:
            return 0

        now = now_utc_iso()
        self.connection.executemany(
            """
            INSERT INTO job_postings (id, title, description, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            [(posting.id, posting.title, posting.description, now) for posting in postings],
        )
        self.connection.commit()
        return len(postings)

    def list_postings(self, limit: int) -> list[StoredPosting]:
        cursor = self.connection.execute(
            """
            SELECT id, title, description, updated_at
            FROM job_postings
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [StoredPosting(**dict(row)) for row in cursor.fetchall()]

    def record_recommendations(
        self,
        resume_text: str,
        recommendations: list[RankedRecommendation],
    ) -> tuple[int, str]:
        generated_at = now_utc_iso()
        cursor = self.connection.execute(
            """
            INSERT INTO recommendation_runs (resume_text, generated_at)
            VALUES (?, ?)
            """,
            (resume_text, generated_at),
        )
        run_id = int(cursor.lastrowid)
        self.connection.executemany(
            """
            INSERT INTO recommendation_items (run_id, job_id, title, score, rank)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (run_id, recommendation.id, recommendation.title, recommendation.score, rank)
                for rank, recommendation in enumerate(recommendations, start=1)
            ],
        )
        self.connection.commit()
        return run_id, generated_at

    def list_recommendation_runs(self, limit: int) -> list[RecommendationRun]:
        cursor = self.connection.execute(
            """
            SELECT
                r.id AS run_id,
                r.generated_at AS generated_at,
                COUNT(i.id) AS recommendation_count
            FROM recommendation_runs r
            LEFT JOIN recommendation_items i ON i.run_id = r.id
            GROUP BY r.id, r.generated_at
            ORDER BY r.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [RecommendationRun(**dict(row)) for row in cursor.fetchall()]


def rank_postings(resume_text: str, postings: list[JobPosting]) -> list[RankedRecommendation]:
    resume_tokens = tokenize(resume_text)
    ranked: list[RankedRecommendation] = []

    for posting in postings:
        job_tokens = tokenize(f"{posting.title} {posting.description}")
        if not job_tokens:
            score = 0.0
        else:
            score = len(resume_tokens.intersection(job_tokens)) / len(job_tokens)

        ranked.append(
            RankedRecommendation(
                id=posting.id,
                title=posting.title,
                score=round(score, 4),
            )
        )

    ranked.sort(key=lambda item: item.score, reverse=True)
    return ranked


def create_app(*, database_path: str | None = None) -> FastAPI:
    resolved_path = database_path or os.getenv("RECOMMENDER_DB_PATH", DEFAULT_DB_PATH)
    repository = RecommenderRepository(database_path=resolved_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await run_in_threadpool(repository.connect)
        app.state.repository = repository
        try:
            yield
        finally:
            await run_in_threadpool(repository.close)

    app = FastAPI(title="OperationBattleship Recommender", version="0.2.0", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "recommender"}

    @app.post("/postings", response_model=UpsertPostingsResponse)
    async def upsert_postings(
        payload: UpsertPostingsRequest,
        request: Request,
    ) -> UpsertPostingsResponse:
        updated = await run_in_threadpool(
            request.app.state.repository.upsert_postings,
            payload.postings,
        )
        return UpsertPostingsResponse(updated=updated)

    @app.get("/postings", response_model=list[StoredPosting])
    async def list_postings(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[StoredPosting]:
        return await run_in_threadpool(request.app.state.repository.list_postings, limit)

    @app.get("/recommendations/history", response_model=RecommendationHistoryResponse)
    async def recommendation_history(
        request: Request,
        limit: int = Query(default=25, ge=1, le=200),
    ) -> RecommendationHistoryResponse:
        runs = await run_in_threadpool(request.app.state.repository.list_recommendation_runs, limit)
        return RecommendationHistoryResponse(runs=runs)

    @app.post("/recommend", response_model=RecommendResponse)
    async def recommend(payload: RecommendRequest, request: Request) -> RecommendResponse:
        source: Literal["payload", "stored"] = "payload"
        postings = payload.postings
        if not postings:
            source = "stored"
            postings = await run_in_threadpool(
                request.app.state.repository.list_postings,
                payload.max_postings,
            )
            postings = [
                JobPosting(id=posting.id, title=posting.title, description=posting.description)
                for posting in postings
            ]
        else:
            await run_in_threadpool(request.app.state.repository.upsert_postings, postings)

        ranked = rank_postings(payload.resume_text, postings)
        run_id, generated_at = await run_in_threadpool(
            request.app.state.repository.record_recommendations,
            payload.resume_text,
            ranked,
        )
        return RecommendResponse(
            run_id=run_id,
            source=source,
            generated_at=generated_at,
            recommendations=ranked,
        )

    return app


app = create_app()

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from urllib import request as urllib_request

from common.utils import now_utc_iso, tokenize
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, HttpUrl, model_validator

DEFAULT_DB_PATH = os.path.join(tempfile.gettempdir(), "operation-battleship", "recommender.sqlite3")

SOURCE_INLINE_JSON = "inline_json"
SOURCE_JSON_URL = "json_url"
SOURCE_TYPES = (SOURCE_INLINE_JSON, SOURCE_JSON_URL)


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


class IngestedPosting(BaseModel):
    id: str | None = None
    title: str = Field(..., min_length=1)
    description: str = Field(default="")


class JobSourceUpsertRequest(BaseModel):
    source_id: str = Field(..., min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=120)
    source_type: Literal["inline_json", "json_url"]
    enabled: bool = True
    postings: list[IngestedPosting] = Field(default_factory=list)
    url: HttpUrl | None = None

    @model_validator(mode="after")
    def validate_source_config(self) -> JobSourceUpsertRequest:
        if self.source_type == SOURCE_INLINE_JSON and not self.postings:
            raise ValueError("Inline source must include at least one posting.")
        if self.source_type == SOURCE_JSON_URL and self.url is None:
            raise ValueError("json_url source must include a url.")
        return self

    def config_json(self) -> str:
        if self.source_type == SOURCE_INLINE_JSON:
            postings = [posting.model_dump() for posting in self.postings]
            return json.dumps({"postings": postings})
        return json.dumps({"url": str(self.url)})


class JobSource(BaseModel):
    source_id: str
    name: str
    source_type: Literal["inline_json", "json_url"]
    enabled: bool
    created_at: str
    updated_at: str
    last_scan_at: str | None = None
    last_status: str | None = None
    last_error: str | None = None
    config: dict[str, Any]


class JobSourceScanResult(BaseModel):
    source_id: str
    scanned_at: str
    status: Literal["ok", "error"]
    fetched: int
    ingested: int
    error: str | None = None


class JobSourceScanBatchResponse(BaseModel):
    scanned_at: str
    requested_sources: int
    successful_sources: int
    failed_sources: int
    total_ingested: int
    results: list[JobSourceScanResult]


class RecommenderRepository:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not initialized")
        return self._connection

    def connect(self) -> None:
        with self._lock:
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

                CREATE TABLE IF NOT EXISTS job_sources (
                    source_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_scan_at TEXT,
                    last_status TEXT,
                    last_error TEXT
                );
                """
            )
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            self._connection.close()
            self._connection = None

    def upsert_postings(self, postings: list[JobPosting]) -> int:
        with self._lock:
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
        with self._lock:
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
        with self._lock:
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
        with self._lock:
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

    def upsert_job_source(self, payload: JobSourceUpsertRequest) -> JobSource:
        with self._lock:
            now = now_utc_iso()
            self.connection.execute(
                """
                INSERT INTO job_sources (
                    source_id,
                    name,
                    source_type,
                    config_json,
                    enabled,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    name = excluded.name,
                    source_type = excluded.source_type,
                    config_json = excluded.config_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    payload.source_id,
                    payload.name,
                    payload.source_type,
                    payload.config_json(),
                    int(payload.enabled),
                    now,
                    now,
                ),
            )
            self.connection.commit()
            return self.get_job_source_or_raise(payload.source_id)

    def get_job_source_or_raise(self, source_id: str) -> JobSource:
        source = self.get_job_source(source_id)
        if source is None:
            raise KeyError(f"Unknown source_id: {source_id}")
        return source

    def get_job_source(self, source_id: str) -> JobSource | None:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    source_id,
                    name,
                    source_type,
                    config_json,
                    enabled,
                    created_at,
                    updated_at,
                    last_scan_at,
                    last_status,
                    last_error
                FROM job_sources
                WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()
            if row is None:
                return None
            return self._to_job_source(row)

    def list_job_sources(self, enabled_only: bool = False) -> list[JobSource]:
        with self._lock:
            if enabled_only:
                cursor = self.connection.execute(
                    """
                    SELECT
                        source_id,
                        name,
                        source_type,
                        config_json,
                        enabled,
                        created_at,
                        updated_at,
                        last_scan_at,
                        last_status,
                        last_error
                    FROM job_sources
                    WHERE enabled = 1
                    ORDER BY source_id
                    """
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT
                        source_id,
                        name,
                        source_type,
                        config_json,
                        enabled,
                        created_at,
                        updated_at,
                        last_scan_at,
                        last_status,
                        last_error
                    FROM job_sources
                    ORDER BY source_id
                    """
                )
            return [self._to_job_source(row) for row in cursor.fetchall()]

    def update_job_source_scan_state(
        self,
        source_id: str,
        *,
        scanned_at: str,
        status: str,
        error: str | None,
    ) -> None:
        with self._lock:
            self.connection.execute(
                """
                UPDATE job_sources
                SET
                    last_scan_at = ?,
                    last_status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ?
                """,
                (
                    scanned_at,
                    status,
                    error,
                    now_utc_iso(),
                    source_id,
                ),
            )
            self.connection.commit()

    def _to_job_source(self, row: sqlite3.Row) -> JobSource:
        config: dict[str, Any] = json.loads(row["config_json"])
        return JobSource(
            source_id=row["source_id"],
            name=row["name"],
            source_type=row["source_type"],
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_scan_at=row["last_scan_at"],
            last_status=row["last_status"],
            last_error=row["last_error"],
            config=config,
        )


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


def to_job_postings_from_payload(source_id: str, payload: Any) -> list[JobPosting]:
    if isinstance(payload, dict):
        raw_postings = payload.get("postings", [])
    elif isinstance(payload, list):
        raw_postings = payload
    else:
        raise ValueError("Source payload must be a JSON object or list.")

    if not isinstance(raw_postings, list):
        raise ValueError("Source payload postings must be a list.")

    postings: list[JobPosting] = []
    for index, item in enumerate(raw_postings, start=1):
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        description = str(item.get("description", "")).strip() or title
        if not title:
            continue
        raw_id = item.get("id")
        external_id = str(raw_id).strip() if raw_id is not None else ""
        if external_id:
            posting_id = external_id
        else:
            digest = hashlib.sha1(f"{source_id}|{title}|{description}".encode()).hexdigest()
            posting_id = f"{source_id}-{index}-{digest[:10]}"
        postings.append(JobPosting(id=posting_id, title=title, description=description))
    return postings


def load_source_payload(source: JobSource) -> Any:
    if source.source_type not in SOURCE_TYPES:
        raise ValueError(f"Unsupported source type: {source.source_type}")

    if source.source_type == SOURCE_INLINE_JSON:
        return source.config.get("postings", [])

    url = str(source.config.get("url", "")).strip()
    if not url:
        raise ValueError("Missing url in job source config.")
    with urllib_request.urlopen(url, timeout=15) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def scan_source(repository: RecommenderRepository, source: JobSource) -> JobSourceScanResult:
    scanned_at = now_utc_iso()
    try:
        payload = load_source_payload(source)
        postings = to_job_postings_from_payload(source.source_id, payload)
        ingested = repository.upsert_postings(postings)
        result = JobSourceScanResult(
            source_id=source.source_id,
            scanned_at=scanned_at,
            status="ok",
            fetched=len(postings),
            ingested=ingested,
        )
        repository.update_job_source_scan_state(
            source.source_id,
            scanned_at=scanned_at,
            status="ok",
            error=None,
        )
        return result
    except Exception as exc:
        error_text = str(exc)
        repository.update_job_source_scan_state(
            source.source_id,
            scanned_at=scanned_at,
            status="error",
            error=error_text,
        )
        return JobSourceScanResult(
            source_id=source.source_id,
            scanned_at=scanned_at,
            status="error",
            fetched=0,
            ingested=0,
            error=error_text,
        )


def create_app(
    *,
    database_path: str | None = None,
    api_key: str | None = None,
) -> FastAPI:
    resolved_path = database_path or os.getenv("RECOMMENDER_DB_PATH", DEFAULT_DB_PATH)
    resolved_api_key = (api_key or os.getenv("RECOMMENDER_API_KEY", "")).strip() or None
    repository = RecommenderRepository(database_path=resolved_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await run_in_threadpool(repository.connect)
        app.state.repository = repository
        app.state.api_key = resolved_api_key
        try:
            yield
        finally:
            await run_in_threadpool(repository.close)

    app = FastAPI(title="OperationBattleship Recommender", version="0.3.0", lifespan=lifespan)

    def require_api_key(request: Request) -> None:
        expected = request.app.state.api_key
        if not expected:
            return
        provided = request.headers.get("x-api-key", "")
        if provided != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "recommender"}

    @app.post("/postings", response_model=UpsertPostingsResponse)
    async def upsert_postings(
        payload: UpsertPostingsRequest,
        request: Request,
    ) -> UpsertPostingsResponse:
        require_api_key(request)
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

    @app.post("/job-sources", response_model=JobSource)
    async def upsert_job_source(payload: JobSourceUpsertRequest, request: Request) -> JobSource:
        require_api_key(request)
        return await run_in_threadpool(request.app.state.repository.upsert_job_source, payload)

    @app.get("/job-sources", response_model=list[JobSource])
    async def list_job_sources(
        request: Request,
        enabled_only: bool = Query(default=False),
    ) -> list[JobSource]:
        return await run_in_threadpool(request.app.state.repository.list_job_sources, enabled_only)

    @app.post("/job-sources/{source_id}/scan", response_model=JobSourceScanResult)
    async def scan_job_source(source_id: str, request: Request) -> JobSourceScanResult:
        require_api_key(request)
        source = await run_in_threadpool(request.app.state.repository.get_job_source, source_id)
        if source is None:
            raise HTTPException(status_code=404, detail="Unknown source_id")

        result = await run_in_threadpool(scan_source, request.app.state.repository, source)
        if result.status == "error":
            raise HTTPException(
                status_code=502,
                detail={"source_id": result.source_id, "error": result.error},
            )
        return result

    @app.post("/job-sources/scan", response_model=JobSourceScanBatchResponse)
    async def scan_job_sources(
        request: Request,
        enabled_only: bool = Query(default=True),
    ) -> JobSourceScanBatchResponse:
        require_api_key(request)
        sources = await run_in_threadpool(
            request.app.state.repository.list_job_sources,
            enabled_only,
        )
        results: list[JobSourceScanResult] = []
        for source in sources:
            result = await run_in_threadpool(
                scan_source,
                request.app.state.repository,
                source,
            )
            results.append(result)

        return JobSourceScanBatchResponse(
            scanned_at=now_utc_iso(),
            requested_sources=len(sources),
            successful_sources=sum(1 for result in results if result.status == "ok"),
            failed_sources=sum(1 for result in results if result.status == "error"),
            total_ingested=sum(result.ingested for result in results),
            results=results,
        )

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

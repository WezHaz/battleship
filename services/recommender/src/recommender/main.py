from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib import request as urllib_request
from urllib.parse import urlsplit

from common.utils import now_utc_iso, tokenize
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, HttpUrl, model_validator

DEFAULT_DB_PATH = os.path.join(tempfile.gettempdir(), "operation-battleship", "recommender.sqlite3")

SOURCE_INLINE_JSON = "inline_json"
SOURCE_JSON_URL = "json_url"
SOURCE_TYPES = (SOURCE_INLINE_JSON, SOURCE_JSON_URL)
LOGGER = logging.getLogger("battleship.recommender")


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def normalize_text(text: str) -> str:
    squashed = normalize_whitespace(text).lower()
    alnum_only = re.sub(r"[^a-z0-9\s]+", " ", squashed)
    return normalize_whitespace(alnum_only)


def normalize_url(url: str | None) -> str:
    if not url:
        return ""
    parsed = urlsplit(url.strip())
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return f"{host}{path}"


def build_dedup_key(
    title: str,
    company: str | None,
    location: str | None,
    apply_url: str | None,
) -> str:
    normalized_title = normalize_text(title)
    normalized_company = normalize_text(company or "")
    normalized_location = normalize_text(location or "")
    normalized_apply_url = normalize_url(apply_url)
    key_input = "|".join(
        [
            normalized_title,
            normalized_company,
            normalized_location,
            normalized_apply_url,
        ]
    )
    return hashlib.sha1(key_input.encode()).hexdigest()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def parse_api_tokens(raw: str) -> dict[str, set[str]]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("RECOMMENDER_API_TOKENS_JSON must be a JSON object.")

    token_map: dict[str, set[str]] = {}
    for token, scopes_value in parsed.items():
        if not isinstance(token, str) or not token.strip():
            raise ValueError("Token keys must be non-empty strings.")
        if isinstance(scopes_value, str):
            scopes = {scopes_value.strip()} if scopes_value.strip() else set()
        elif isinstance(scopes_value, list):
            scopes = {
                str(scope).strip()
                for scope in scopes_value
                if isinstance(scope, str) and scope.strip()
            }
        else:
            raise ValueError("Token scopes must be a string or list of strings.")
        token_map[token] = scopes
    return token_map


def build_auth_subject(token: str) -> str:
    token_digest = hashlib.sha1(token.encode()).hexdigest()[:12]
    return f"token:{token_digest}"


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def normalize_scopes(scopes: list[str] | set[str]) -> list[str]:
    return sorted({scope.strip() for scope in scopes if scope.strip()})


class JobPosting(BaseModel):
    id: str = Field(..., description="Unique job identifier")
    title: str
    description: str
    company: str | None = None
    location: str | None = None
    apply_url: str | None = None
    source_id: str | None = None
    external_id: str | None = None
    updated_at: str | None = None
    dedup_key: str | None = None
    duplicate_hint_count: int = 0


class RecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[JobPosting] = Field(default_factory=list)
    max_postings: int = Field(default=100, ge=1, le=500)
    profile_id: str | None = None
    preferred_keywords: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    preferred_companies: list[str] = Field(default_factory=list)
    remote_only: bool | None = None


class ScoreBreakdown(BaseModel):
    title_overlap: float
    description_overlap: float
    preferred_keyword_overlap: float
    preference_bonus: float
    freshness_bonus: float
    duplicate_penalty: float
    final_score: float


class RankedRecommendation(BaseModel):
    id: str
    title: str
    company: str | None = None
    location: str | None = None
    apply_url: str | None = None
    score: float
    matched_terms: list[str] = Field(default_factory=list)
    score_breakdown: ScoreBreakdown


class RecommendResponse(BaseModel):
    run_id: int
    source: Literal["payload", "stored"]
    applied_profile_id: str | None = None
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
    company: str | None = None
    location: str | None = None
    apply_url: str | None = None
    source_id: str | None = None
    external_id: str | None = None
    dedup_key: str
    duplicate_hint_count: int
    updated_at: str


class RecommendationRun(BaseModel):
    run_id: int
    generated_at: str
    recommendation_count: int


class RecommendationHistoryResponse(BaseModel):
    runs: list[RecommendationRun]


class UserProfileUpsertRequest(BaseModel):
    profile_id: str = Field(..., min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=120)
    preferred_keywords: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    preferred_companies: list[str] = Field(default_factory=list)
    remote_only: bool = False

    def config_json(self) -> str:
        preferred_keywords = [
            normalize_whitespace(value) for value in self.preferred_keywords if value.strip()
        ]
        preferred_locations = [
            normalize_whitespace(value) for value in self.preferred_locations if value.strip()
        ]
        preferred_companies = [
            normalize_whitespace(value) for value in self.preferred_companies if value.strip()
        ]
        return json.dumps(
            {
                "preferred_keywords": preferred_keywords,
                "preferred_locations": preferred_locations,
                "preferred_companies": preferred_companies,
                "remote_only": self.remote_only,
            }
        )


class UserPreferenceProfile(BaseModel):
    profile_id: str
    name: str
    preferred_keywords: list[str]
    preferred_locations: list[str]
    preferred_companies: list[str]
    remote_only: bool
    created_at: str
    updated_at: str


class IngestedPosting(BaseModel):
    id: str | None = None
    external_id: str | None = None
    title: str = Field(..., min_length=1)
    description: str = Field(default="")
    company: str | None = None
    location: str | None = None
    apply_url: str | None = None


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
    last_success_at: str | None = None
    last_status: str | None = None
    last_error: str | None = None
    next_eligible_scan_at: str | None = None
    consecutive_failures: int = 0
    config: dict[str, Any]


class JobSourceScanResult(BaseModel):
    source_id: str
    scanned_at: str
    trigger: Literal["manual", "scheduled"]
    status: Literal["ok", "error", "skipped"]
    fetched: int
    ingested: int
    possible_duplicates: int
    attempt_number: int
    backoff_seconds: int
    next_eligible_scan_at: str | None = None
    error: str | None = None


class JobSourceScanBatchResponse(BaseModel):
    scanned_at: str
    trigger: Literal["manual", "scheduled"]
    respect_backoff: bool
    requested_sources: int
    successful_sources: int
    failed_sources: int
    skipped_sources: int
    total_ingested: int
    possible_duplicates: int
    results: list[JobSourceScanResult]


class JobSourceScanHistoryItem(BaseModel):
    history_id: int
    source_id: str
    scanned_at: str
    trigger: Literal["manual", "scheduled"]
    status: Literal["ok", "error", "skipped"]
    fetched: int
    ingested: int
    possible_duplicates: int
    attempt_number: int
    backoff_seconds: int
    next_eligible_scan_at: str | None = None
    respect_backoff: bool
    error: str | None = None


class UpsertSummary(BaseModel):
    updated: int
    possible_duplicates: int


class AuditEvent(BaseModel):
    event_id: int
    request_id: str | None = None
    occurred_at: str
    method: str
    path: str
    action: str
    scope: str | None = None
    source_ip: str | None = None
    user_agent: str | None = None
    auth_subject: str | None = None
    status: str
    message: str | None = None


class ApiTokenCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    scopes: list[str] = Field(default_factory=list)
    expires_in_days: int | None = Field(default=None, ge=1, le=3650)
    expires_at: str | None = None
    notes: str | None = Field(default=None, max_length=500)

    @model_validator(mode="after")
    def validate_expiry(self) -> ApiTokenCreateRequest:
        if self.expires_at:
            parsed = parse_iso_datetime(self.expires_at)
            if parsed is None:
                raise ValueError("expires_at must be an ISO-8601 datetime string.")
        return self


class ApiTokenMetadata(BaseModel):
    token_id: str
    name: str
    scopes: list[str]
    created_at: str
    updated_at: str
    expires_at: str | None
    revoked_at: str | None
    last_used_at: str | None
    last_used_ip: str | None
    last_used_user_agent: str | None
    notes: str | None
    active: bool


class ApiTokenCreateResponse(BaseModel):
    token: str
    metadata: ApiTokenMetadata


class TokenAuthContext(BaseModel):
    scopes: set[str]
    auth_subject: str
    token_id: str | None = None


class MetricsSnapshot(BaseModel):
    generated_at: str
    totals: dict[str, int]
    endpoints: dict[str, dict[str, float | int]]


class MetricsStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._totals = {"requests": 0, "errors": 0}
        self._endpoints: dict[str, dict[str, float | int]] = {}

    def observe(self, *, method: str, path: str, status_code: int, duration_ms: float) -> None:
        key = f"{method} {path}"
        bucket = f"{status_code // 100}xx"
        with self._lock:
            self._totals["requests"] += 1
            if status_code >= 400:
                self._totals["errors"] += 1
            endpoint = self._endpoints.setdefault(
                key,
                {
                    "count": 0,
                    "2xx": 0,
                    "4xx": 0,
                    "5xx": 0,
                    "latency_ms_sum": 0.0,
                    "latency_ms_avg": 0.0,
                },
            )
            endpoint["count"] = int(endpoint["count"]) + 1
            if bucket in ("2xx", "4xx", "5xx"):
                endpoint[bucket] = int(endpoint[bucket]) + 1
            endpoint["latency_ms_sum"] = float(endpoint["latency_ms_sum"]) + duration_ms
            endpoint["latency_ms_avg"] = (
                float(endpoint["latency_ms_sum"]) / int(endpoint["count"])
            )

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                generated_at=now_utc_iso(),
                totals=dict(self._totals),
                endpoints={key: dict(value) for key, value in self._endpoints.items()},
            )


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
                    company TEXT,
                    location TEXT,
                    apply_url TEXT,
                    source_id TEXT,
                    external_id TEXT,
                    normalized_title TEXT NOT NULL DEFAULT '',
                    normalized_company TEXT NOT NULL DEFAULT '',
                    normalized_location TEXT NOT NULL DEFAULT '',
                    normalized_url TEXT NOT NULL DEFAULT '',
                    dedup_key TEXT NOT NULL DEFAULT '',
                    duplicate_hint_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
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
                    last_success_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    next_eligible_scan_at TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS job_source_scan_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    scanned_at TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fetched INTEGER NOT NULL,
                    ingested INTEGER NOT NULL,
                    possible_duplicates INTEGER NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    backoff_seconds INTEGER NOT NULL,
                    next_eligible_scan_at TEXT,
                    respect_backoff INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS user_profiles (
                    profile_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_tokens (
                    token_id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    scopes_json TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT,
                    revoked_at TEXT,
                    last_used_at TEXT,
                    last_used_ip TEXT,
                    last_used_user_agent TEXT
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    request_id TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    action TEXT NOT NULL,
                    scope TEXT,
                    source_ip TEXT,
                    user_agent TEXT,
                    auth_subject TEXT,
                    status TEXT NOT NULL,
                    message TEXT
                );
                """
            )
            self._ensure_job_postings_columns()
            self._ensure_job_sources_columns()
            self._ensure_audit_events_columns()
            self._ensure_api_tokens_columns()
            self._connection.commit()

    def _ensure_job_postings_columns(self) -> None:
        column_rows = self.connection.execute("PRAGMA table_info(job_postings)").fetchall()
        existing = {row["name"] for row in column_rows}
        required_definitions = {
            "company": "TEXT",
            "location": "TEXT",
            "apply_url": "TEXT",
            "source_id": "TEXT",
            "external_id": "TEXT",
            "normalized_title": "TEXT NOT NULL DEFAULT ''",
            "normalized_company": "TEXT NOT NULL DEFAULT ''",
            "normalized_location": "TEXT NOT NULL DEFAULT ''",
            "normalized_url": "TEXT NOT NULL DEFAULT ''",
            "dedup_key": "TEXT NOT NULL DEFAULT ''",
            "duplicate_hint_count": "INTEGER NOT NULL DEFAULT 0",
            "created_at": "TEXT NOT NULL DEFAULT ''",
        }
        for column_name, definition in required_definitions.items():
            if column_name in existing:
                continue
            self.connection.execute(
                f"ALTER TABLE job_postings ADD COLUMN {column_name} {definition}"
            )

    def _ensure_job_sources_columns(self) -> None:
        column_rows = self.connection.execute("PRAGMA table_info(job_sources)").fetchall()
        existing = {row["name"] for row in column_rows}
        required_definitions = {
            "last_success_at": "TEXT",
            "next_eligible_scan_at": "TEXT",
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
        }
        for column_name, definition in required_definitions.items():
            if column_name in existing:
                continue
            self.connection.execute(
                f"ALTER TABLE job_sources ADD COLUMN {column_name} {definition}"
            )

    def _ensure_audit_events_columns(self) -> None:
        column_rows = self.connection.execute("PRAGMA table_info(audit_events)").fetchall()
        existing = {row["name"] for row in column_rows}
        if "request_id" not in existing:
            self.connection.execute("ALTER TABLE audit_events ADD COLUMN request_id TEXT")

    def _ensure_api_tokens_columns(self) -> None:
        column_rows = self.connection.execute("PRAGMA table_info(api_tokens)").fetchall()
        existing = {row["name"] for row in column_rows}
        required_definitions = {
            "notes": "TEXT",
            "revoked_at": "TEXT",
            "last_used_at": "TEXT",
            "last_used_ip": "TEXT",
            "last_used_user_agent": "TEXT",
        }
        for column_name, definition in required_definitions.items():
            if column_name in existing:
                continue
            self.connection.execute(
                f"ALTER TABLE api_tokens ADD COLUMN {column_name} {definition}"
            )

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            self._connection.close()
            self._connection = None

    def upsert_postings(
        self,
        postings: list[JobPosting],
        *,
        return_stats: bool = False,
    ) -> int | UpsertSummary:
        with self._lock:
            if not postings:
                if return_stats:
                    return UpsertSummary(updated=0, possible_duplicates=0)
                return 0

            now = now_utc_iso()
            possible_duplicates = 0
            for posting in postings:
                title = normalize_whitespace(posting.title)
                description = normalize_whitespace(posting.description) or title
                company = normalize_whitespace(posting.company or "") or None
                location = normalize_whitespace(posting.location or "") or None
                apply_url = (posting.apply_url or "").strip() or None
                source_id = (posting.source_id or "").strip() or None
                external_id = (posting.external_id or "").strip() or None

                normalized_title = normalize_text(title)
                normalized_company = normalize_text(company or "")
                normalized_location = normalize_text(location or "")
                normalized_url = normalize_url(apply_url)
                dedup_key = posting.dedup_key or build_dedup_key(
                    title,
                    company,
                    location,
                    apply_url,
                )

                duplicate_hint_count = int(
                    self.connection.execute(
                        """
                        SELECT COUNT(1) AS c
                        FROM job_postings
                        WHERE dedup_key = ? AND id != ?
                        """,
                        (dedup_key, posting.id),
                    ).fetchone()["c"]
                )
                if duplicate_hint_count > 0:
                    possible_duplicates += 1

                self.connection.execute(
                    """
                    INSERT INTO job_postings (
                        id,
                        title,
                        description,
                        company,
                        location,
                        apply_url,
                        source_id,
                        external_id,
                        normalized_title,
                        normalized_company,
                        normalized_location,
                        normalized_url,
                        dedup_key,
                        duplicate_hint_count,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        company = excluded.company,
                        location = excluded.location,
                        apply_url = excluded.apply_url,
                        source_id = excluded.source_id,
                        external_id = excluded.external_id,
                        normalized_title = excluded.normalized_title,
                        normalized_company = excluded.normalized_company,
                        normalized_location = excluded.normalized_location,
                        normalized_url = excluded.normalized_url,
                        dedup_key = excluded.dedup_key,
                        duplicate_hint_count = excluded.duplicate_hint_count,
                        updated_at = excluded.updated_at
                    """,
                    (
                        posting.id,
                        title,
                        description,
                        company,
                        location,
                        apply_url,
                        source_id,
                        external_id,
                        normalized_title,
                        normalized_company,
                        normalized_location,
                        normalized_url,
                        dedup_key,
                        duplicate_hint_count,
                        now,
                        now,
                    ),
                )

            self.connection.commit()
            if return_stats:
                return UpsertSummary(updated=len(postings), possible_duplicates=possible_duplicates)
            return len(postings)

    def list_postings(self, limit: int) -> list[StoredPosting]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    id,
                    title,
                    description,
                    company,
                    location,
                    apply_url,
                    source_id,
                    external_id,
                    dedup_key,
                    duplicate_hint_count,
                    updated_at
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
                    last_success_at,
                    last_status,
                    last_error,
                    next_eligible_scan_at,
                    consecutive_failures
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
                        last_success_at,
                        last_status,
                        last_error,
                        next_eligible_scan_at,
                        consecutive_failures
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
                        last_success_at,
                        last_status,
                        last_error,
                        next_eligible_scan_at,
                        consecutive_failures
                    FROM job_sources
                    ORDER BY source_id
                    """
                )
            return [self._to_job_source(row) for row in cursor.fetchall()]

    def list_scan_targets(
        self,
        *,
        enabled_only: bool,
        respect_backoff: bool,
        now_iso: str,
    ) -> list[JobSource]:
        with self._lock:
            query = """
                SELECT
                    source_id,
                    name,
                    source_type,
                    config_json,
                    enabled,
                    created_at,
                    updated_at,
                    last_scan_at,
                    last_success_at,
                    last_status,
                    last_error,
                    next_eligible_scan_at,
                    consecutive_failures
                FROM job_sources
            """
            filters: list[str] = []
            params: list[Any] = []
            if enabled_only:
                filters.append("enabled = 1")
            if respect_backoff:
                filters.append("(next_eligible_scan_at IS NULL OR next_eligible_scan_at <= ?)")
                params.append(now_iso)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += " ORDER BY source_id"
            cursor = self.connection.execute(query, tuple(params))
            return [self._to_job_source(row) for row in cursor.fetchall()]

    def upsert_user_profile(self, payload: UserProfileUpsertRequest) -> UserPreferenceProfile:
        with self._lock:
            now = now_utc_iso()
            self.connection.execute(
                """
                INSERT INTO user_profiles (
                    profile_id,
                    name,
                    config_json,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(profile_id) DO UPDATE SET
                    name = excluded.name,
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload.profile_id,
                    payload.name,
                    payload.config_json(),
                    now,
                    now,
                ),
            )
            self.connection.commit()
            return self.get_user_profile_or_raise(payload.profile_id)

    def get_user_profile_or_raise(self, profile_id: str) -> UserPreferenceProfile:
        profile = self.get_user_profile(profile_id)
        if profile is None:
            raise KeyError(f"Unknown profile_id: {profile_id}")
        return profile

    def get_user_profile(self, profile_id: str) -> UserPreferenceProfile | None:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    profile_id,
                    name,
                    config_json,
                    created_at,
                    updated_at
                FROM user_profiles
                WHERE profile_id = ?
                """,
                (profile_id,),
            ).fetchone()
            if row is None:
                return None
            return self._to_user_profile(row)

    def list_user_profiles(self) -> list[UserPreferenceProfile]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    profile_id,
                    name,
                    config_json,
                    created_at,
                    updated_at
                FROM user_profiles
                ORDER BY profile_id
                """
            )
            return [self._to_user_profile(row) for row in cursor.fetchall()]

    def delete_user_profile(self, profile_id: str) -> bool:
        with self._lock:
            cursor = self.connection.execute(
                "DELETE FROM user_profiles WHERE profile_id = ?",
                (profile_id,),
            )
            self.connection.commit()
            return cursor.rowcount > 0

    def has_active_api_tokens(self) -> bool:
        with self._lock:
            now = now_utc_iso()
            count = int(
                self.connection.execute(
                    """
                    SELECT COUNT(1) AS c
                    FROM api_tokens
                    WHERE revoked_at IS NULL
                      AND (expires_at IS NULL OR expires_at > ?)
                    """,
                    (now,),
                ).fetchone()["c"]
            )
            return count > 0

    def create_api_token(self, payload: ApiTokenCreateRequest) -> ApiTokenCreateResponse:
        with self._lock:
            now = now_utc_iso()
            token_id = str(uuid.uuid4())
            raw_token = f"obs_{secrets.token_urlsafe(32)}"
            token_hash = hash_token(raw_token)
            scopes = normalize_scopes(payload.scopes)
            expires_at: str | None = None
            if payload.expires_at:
                parsed = parse_iso_datetime(payload.expires_at)
                if parsed is None:
                    raise ValueError("expires_at must be a valid ISO-8601 datetime.")
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                expires_at = parsed.astimezone(UTC).isoformat()
            elif payload.expires_in_days:
                expires = datetime.now(UTC) + timedelta(days=payload.expires_in_days)
                expires_at = expires.isoformat()

            self.connection.execute(
                """
                INSERT INTO api_tokens (
                    token_id,
                    token_hash,
                    name,
                    scopes_json,
                    notes,
                    created_at,
                    updated_at,
                    expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token_id,
                    token_hash,
                    payload.name,
                    json.dumps(scopes),
                    payload.notes,
                    now,
                    now,
                    expires_at,
                ),
            )
            self.connection.commit()
            metadata = self.get_api_token_or_raise(token_id)
            return ApiTokenCreateResponse(token=raw_token, metadata=metadata)

    def get_api_token_or_raise(self, token_id: str) -> ApiTokenMetadata:
        token = self.get_api_token(token_id)
        if token is None:
            raise KeyError(f"Unknown token_id: {token_id}")
        return token

    def get_api_token(self, token_id: str) -> ApiTokenMetadata | None:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    token_id,
                    name,
                    scopes_json,
                    notes,
                    created_at,
                    updated_at,
                    expires_at,
                    revoked_at,
                    last_used_at,
                    last_used_ip,
                    last_used_user_agent
                FROM api_tokens
                WHERE token_id = ?
                """,
                (token_id,),
            ).fetchone()
            if row is None:
                return None
            return self._to_api_token_metadata(row)

    def list_api_tokens(self, *, include_revoked: bool) -> list[ApiTokenMetadata]:
        with self._lock:
            if include_revoked:
                cursor = self.connection.execute(
                    """
                    SELECT
                        token_id,
                        name,
                        scopes_json,
                        notes,
                        created_at,
                        updated_at,
                        expires_at,
                        revoked_at,
                        last_used_at,
                        last_used_ip,
                        last_used_user_agent
                    FROM api_tokens
                    ORDER BY created_at DESC
                    """
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT
                        token_id,
                        name,
                        scopes_json,
                        notes,
                        created_at,
                        updated_at,
                        expires_at,
                        revoked_at,
                        last_used_at,
                        last_used_ip,
                        last_used_user_agent
                    FROM api_tokens
                    WHERE revoked_at IS NULL
                    ORDER BY created_at DESC
                    """
                )
            return [self._to_api_token_metadata(row) for row in cursor.fetchall()]

    def revoke_api_token(self, token_id: str) -> bool:
        with self._lock:
            now = now_utc_iso()
            cursor = self.connection.execute(
                """
                UPDATE api_tokens
                SET revoked_at = ?, updated_at = ?
                WHERE token_id = ? AND revoked_at IS NULL
                """,
                (now, now, token_id),
            )
            self.connection.commit()
            return cursor.rowcount > 0

    def resolve_db_token(self, token_value: str) -> TokenAuthContext | None:
        with self._lock:
            now = now_utc_iso()
            token_hash = hash_token(token_value)
            row = self.connection.execute(
                """
                SELECT token_id, scopes_json
                FROM api_tokens
                WHERE token_hash = ?
                  AND revoked_at IS NULL
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (token_hash, now),
            ).fetchone()
            if row is None:
                return None
            scopes = set(json.loads(row["scopes_json"]))
            return TokenAuthContext(
                scopes=scopes,
                auth_subject=f"db-token:{row['token_id']}",
                token_id=row["token_id"],
            )

    def touch_api_token_usage(
        self,
        token_id: str,
        *,
        source_ip: str | None,
        user_agent: str | None,
    ) -> None:
        with self._lock:
            now = now_utc_iso()
            self.connection.execute(
                """
                UPDATE api_tokens
                SET
                    last_used_at = ?,
                    last_used_ip = ?,
                    last_used_user_agent = ?,
                    updated_at = ?
                WHERE token_id = ?
                """,
                (now, source_ip, user_agent, now, token_id),
            )
            self.connection.commit()

    def record_audit_event(
        self,
        *,
        request_id: str | None,
        method: str,
        path: str,
        action: str,
        scope: str | None,
        source_ip: str | None,
        user_agent: str | None,
        auth_subject: str | None,
        status: str,
        message: str | None,
    ) -> int:
        with self._lock:
            cursor = self.connection.execute(
                """
                INSERT INTO audit_events (
                    occurred_at,
                    request_id,
                    method,
                    path,
                    action,
                    scope,
                    source_ip,
                    user_agent,
                    auth_subject,
                    status,
                    message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_utc_iso(),
                    request_id,
                    method,
                    path,
                    action,
                    scope,
                    source_ip,
                    user_agent,
                    auth_subject,
                    status,
                    message,
                ),
            )
            self.connection.commit()
            return int(cursor.lastrowid)

    def list_audit_events(
        self,
        *,
        limit: int,
        action: str | None,
        status: str | None,
    ) -> list[AuditEvent]:
        with self._lock:
            query = """
                SELECT
                    id AS event_id,
                    occurred_at,
                    request_id,
                    method,
                    path,
                    action,
                    scope,
                    source_ip,
                    user_agent,
                    auth_subject,
                    status,
                    message
                FROM audit_events
            """
            params: list[Any] = []
            filters: list[str] = []
            if action:
                filters.append("action = ?")
                params.append(action)
            if status:
                filters.append("status = ?")
                params.append(status)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            cursor = self.connection.execute(query, tuple(params))
            return [AuditEvent(**dict(row)) for row in cursor.fetchall()]

    def record_job_source_scan_result(
        self,
        source_id: str,
        *,
        scanned_at: str,
        trigger: str,
        status: str,
        fetched: int,
        ingested: int,
        possible_duplicates: int,
        error: str | None,
        respect_backoff: bool,
    ) -> JobSourceScanResult:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT
                    consecutive_failures,
                    next_eligible_scan_at,
                    last_error
                FROM job_sources
                WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown source_id: {source_id}")

            previous_failures = int(row["consecutive_failures"] or 0)
            next_eligible_previous = row["next_eligible_scan_at"]
            previous_last_error = row["last_error"]
            attempt_number = previous_failures + 1
            backoff_seconds = 0
            next_eligible_scan_at: str | None = None
            last_success_at: str | None = None
            next_failure_count = previous_failures
            last_error = error

            if status == "ok":
                attempt_number = 1
                next_failure_count = 0
                last_success_at = scanned_at
                last_error = None
            elif status == "error":
                next_failure_count = previous_failures + 1
                backoff_seconds = min(60 * (2 ** max(next_failure_count - 1, 0)), 3600)
                next_eligible = datetime.fromisoformat(scanned_at) + timedelta(
                    seconds=backoff_seconds
                )
                next_eligible_scan_at = next_eligible.isoformat()
            elif status == "skipped":
                attempt_number = previous_failures + 1
                next_failure_count = previous_failures
                next_eligible_scan_at = next_eligible_previous
                last_error = previous_last_error
                if next_eligible_scan_at:
                    parsed_now = parse_iso_datetime(scanned_at)
                    parsed_next = parse_iso_datetime(next_eligible_scan_at)
                    if parsed_now and parsed_next:
                        delta = parsed_next - parsed_now
                        backoff_seconds = max(int(delta.total_seconds()), 0)

            self.connection.execute(
                """
                UPDATE job_sources
                SET
                    last_scan_at = ?,
                    last_success_at = COALESCE(?, last_success_at),
                    last_status = ?,
                    last_error = ?,
                    next_eligible_scan_at = ?,
                    consecutive_failures = ?,
                    updated_at = ?
                WHERE source_id = ?
                """,
                (
                    scanned_at,
                    last_success_at,
                    status,
                    last_error,
                    next_eligible_scan_at,
                    next_failure_count,
                    now_utc_iso(),
                    source_id,
                ),
            )

            self.connection.execute(
                """
                INSERT INTO job_source_scan_history (
                    source_id,
                    scanned_at,
                    trigger,
                    status,
                    fetched,
                    ingested,
                    possible_duplicates,
                    attempt_number,
                    backoff_seconds,
                    next_eligible_scan_at,
                    respect_backoff,
                    error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    scanned_at,
                    trigger,
                    status,
                    fetched,
                    ingested,
                    possible_duplicates,
                    attempt_number,
                    backoff_seconds,
                    next_eligible_scan_at,
                    int(respect_backoff),
                    error,
                ),
            )
            self.connection.commit()

            return JobSourceScanResult(
                source_id=source_id,
                scanned_at=scanned_at,
                trigger="scheduled" if trigger == "scheduled" else "manual",
                status=status,  # type: ignore[arg-type]
                fetched=fetched,
                ingested=ingested,
                possible_duplicates=possible_duplicates,
                attempt_number=attempt_number,
                backoff_seconds=backoff_seconds,
                next_eligible_scan_at=next_eligible_scan_at,
                error=error,
            )

    def list_job_source_scan_history(
        self,
        *,
        limit: int,
        offset: int,
        source_id: str | None,
        trigger: str | None,
        status: str | None,
        scanned_after: str | None,
        scanned_before: str | None,
    ) -> list[JobSourceScanHistoryItem]:
        with self._lock:
            query = """
                SELECT
                    id AS history_id,
                    source_id,
                    scanned_at,
                    trigger,
                    status,
                    fetched,
                    ingested,
                    possible_duplicates,
                    attempt_number,
                    backoff_seconds,
                    next_eligible_scan_at,
                    respect_backoff,
                    error
                FROM job_source_scan_history
            """
            params: list[Any] = []
            filters: list[str] = []
            if source_id:
                filters.append("source_id = ?")
                params.append(source_id)
            if trigger:
                filters.append("trigger = ?")
                params.append(trigger)
            if status:
                filters.append("status = ?")
                params.append(status)
            if scanned_after:
                filters.append("scanned_at >= ?")
                params.append(scanned_after)
            if scanned_before:
                filters.append("scanned_at <= ?")
                params.append(scanned_before)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += " ORDER BY id DESC LIMIT ? OFFSET ?"
            params.append(limit)
            params.append(offset)
            cursor = self.connection.execute(query, tuple(params))
            rows = cursor.fetchall()
            return [
                JobSourceScanHistoryItem(
                    history_id=row["history_id"],
                    source_id=row["source_id"],
                    scanned_at=row["scanned_at"],
                    trigger=row["trigger"],
                    status=row["status"],
                    fetched=row["fetched"],
                    ingested=row["ingested"],
                    possible_duplicates=row["possible_duplicates"],
                    attempt_number=row["attempt_number"],
                    backoff_seconds=row["backoff_seconds"],
                    next_eligible_scan_at=row["next_eligible_scan_at"],
                    respect_backoff=bool(row["respect_backoff"]),
                    error=row["error"],
                )
                for row in rows
            ]

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
            last_success_at=row["last_success_at"],
            last_status=row["last_status"],
            last_error=row["last_error"],
            next_eligible_scan_at=row["next_eligible_scan_at"],
            consecutive_failures=int(row["consecutive_failures"] or 0),
            config=config,
        )

    def _to_user_profile(self, row: sqlite3.Row) -> UserPreferenceProfile:
        config: dict[str, Any] = json.loads(row["config_json"])
        return UserPreferenceProfile(
            profile_id=row["profile_id"],
            name=row["name"],
            preferred_keywords=config.get("preferred_keywords", []),
            preferred_locations=config.get("preferred_locations", []),
            preferred_companies=config.get("preferred_companies", []),
            remote_only=bool(config.get("remote_only", False)),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_api_token_metadata(self, row: sqlite3.Row) -> ApiTokenMetadata:
        scopes = json.loads(row["scopes_json"])
        now = now_utc_iso()
        expires_at = row["expires_at"]
        revoked_at = row["revoked_at"]
        active = revoked_at is None and (expires_at is None or expires_at > now)
        return ApiTokenMetadata(
            token_id=row["token_id"],
            name=row["name"],
            scopes=scopes,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            expires_at=expires_at,
            revoked_at=revoked_at,
            last_used_at=row["last_used_at"],
            last_used_ip=row["last_used_ip"],
            last_used_user_agent=row["last_used_user_agent"],
            notes=row["notes"],
            active=active,
        )


def _token_overlap(reference: set[str], candidates: set[str]) -> float:
    if not candidates:
        return 0.0
    return len(reference.intersection(candidates)) / len(candidates)


def _freshness_bonus(updated_at: str | None) -> float:
    updated = parse_iso_datetime(updated_at)
    if updated is None:
        return 0.0
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    age_hours = (datetime.now(UTC) - updated).total_seconds() / 3600
    if age_hours <= 24:
        return 0.06
    if age_hours <= 72:
        return 0.03
    if age_hours <= 168:
        return 0.01
    return 0.0


def rank_postings(
    resume_text: str,
    postings: list[JobPosting],
    *,
    preferred_keywords: list[str] | None = None,
    preferred_locations: list[str] | None = None,
    preferred_companies: list[str] | None = None,
    remote_only: bool = False,
) -> list[RankedRecommendation]:
    resume_tokens = tokenize(resume_text)
    preferred_keyword_tokens = tokenize(" ".join(preferred_keywords or []))
    preferred_locations_normalized = {normalize_text(value) for value in preferred_locations or []}
    preferred_companies_normalized = {normalize_text(value) for value in preferred_companies or []}

    ranked: list[RankedRecommendation] = []
    for posting in postings:
        title_tokens = tokenize(posting.title)
        description_tokens = tokenize(posting.description)
        company_tokens = tokenize(posting.company or "")
        all_job_tokens = title_tokens.union(description_tokens).union(company_tokens)

        title_overlap = _token_overlap(resume_tokens, title_tokens)
        description_overlap = _token_overlap(resume_tokens, description_tokens)
        keyword_overlap = _token_overlap(preferred_keyword_tokens, all_job_tokens)

        preference_bonus = 0.0
        normalized_company = normalize_text(posting.company or "")
        normalized_location = normalize_text(posting.location or "")

        if normalized_company and normalized_company in preferred_companies_normalized:
            preference_bonus += 0.08
        if normalized_location and normalized_location in preferred_locations_normalized:
            preference_bonus += 0.08

        remote_signal = "remote" in normalize_text(
            f"{posting.location or ''} {posting.title} {posting.description}"
        )
        if remote_only:
            preference_bonus += 0.08 if remote_signal else -0.05

        freshness_bonus = _freshness_bonus(posting.updated_at)
        duplicate_penalty = min(0.02 * posting.duplicate_hint_count, 0.08)

        score = (
            0.55 * title_overlap
            + 0.35 * description_overlap
            + 0.10 * keyword_overlap
            + preference_bonus
            + freshness_bonus
            - duplicate_penalty
        )
        score = max(score, 0.0)
        matched_terms = sorted(list(resume_tokens.intersection(all_job_tokens)))[:12]

        breakdown = ScoreBreakdown(
            title_overlap=round(title_overlap, 4),
            description_overlap=round(description_overlap, 4),
            preferred_keyword_overlap=round(keyword_overlap, 4),
            preference_bonus=round(preference_bonus, 4),
            freshness_bonus=round(freshness_bonus, 4),
            duplicate_penalty=round(duplicate_penalty, 4),
            final_score=round(score, 4),
        )
        ranked.append(
            RankedRecommendation(
                id=posting.id,
                title=posting.title,
                company=posting.company,
                location=posting.location,
                apply_url=posting.apply_url,
                score=round(score, 4),
                matched_terms=matched_terms,
                score_breakdown=breakdown,
            )
        )

    ranked.sort(key=lambda item: item.score, reverse=True)
    return ranked


def resolve_recommendation_preferences(
    payload: RecommendRequest,
    profile: UserPreferenceProfile | None,
) -> tuple[list[str], list[str], list[str], bool]:
    profile_keywords = profile.preferred_keywords if profile else []
    profile_locations = profile.preferred_locations if profile else []
    profile_companies = profile.preferred_companies if profile else []
    profile_remote_only = profile.remote_only if profile else False

    preferred_keywords = payload.preferred_keywords or profile_keywords
    preferred_locations = payload.preferred_locations or profile_locations
    preferred_companies = payload.preferred_companies or profile_companies
    remote_only = payload.remote_only if payload.remote_only is not None else profile_remote_only
    return preferred_keywords, preferred_locations, preferred_companies, remote_only


def to_job_postings_from_payload(
    source_id: str,
    payload: Any,
    *,
    scanned_at: str,
) -> list[JobPosting]:
    if isinstance(payload, dict):
        raw_postings = payload.get("postings", [])
    elif isinstance(payload, list):
        raw_postings = payload
    else:
        raise ValueError("Source payload must be a JSON object or list.")

    if not isinstance(raw_postings, list):
        raise ValueError("Source payload postings must be a list.")

    postings: list[JobPosting] = []
    scan_marker = scanned_at.replace("-", "").replace(":", "").replace(".", "")

    for index, item in enumerate(raw_postings, start=1):
        if not isinstance(item, dict):
            continue
        title = normalize_whitespace(str(item.get("title", "")).strip())
        description = normalize_whitespace(str(item.get("description", "")).strip()) or title
        company = normalize_whitespace(str(item.get("company", "")).strip()) or None
        location = normalize_whitespace(str(item.get("location", "")).strip()) or None
        apply_url = str(item.get("apply_url", "")).strip() or None
        if not title:
            continue

        external_id_candidates = [item.get("external_id"), item.get("id")]
        external_id = next(
            (
                str(value).strip()
                for value in external_id_candidates
                if value is not None and str(value).strip()
            ),
            None,
        )

        if external_id:
            posting_id = f"{source_id}::{external_id}"
        else:
            base = "|".join(
                [
                    source_id,
                    title,
                    description,
                    company or "",
                    location or "",
                    apply_url or "",
                    str(index),
                    scan_marker,
                ]
            )
            digest = hashlib.sha1(base.encode()).hexdigest()
            posting_id = f"{source_id}::{digest[:14]}"

        postings.append(
            JobPosting(
                id=posting_id,
                title=title,
                description=description,
                company=company,
                location=location,
                apply_url=apply_url,
                source_id=source_id,
                external_id=external_id,
                updated_at=scanned_at,
                dedup_key=build_dedup_key(title, company, location, apply_url),
            )
        )
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


def scan_source(
    repository: RecommenderRepository,
    source: JobSource,
    *,
    trigger: Literal["manual", "scheduled"],
    respect_backoff: bool,
) -> JobSourceScanResult:
    scanned_at = now_utc_iso()
    if respect_backoff and source.next_eligible_scan_at:
        parsed_now = parse_iso_datetime(scanned_at)
        parsed_next = parse_iso_datetime(source.next_eligible_scan_at)
        if parsed_now and parsed_next and parsed_next > parsed_now:
            return repository.record_job_source_scan_result(
                source.source_id,
                scanned_at=scanned_at,
                trigger=trigger,
                status="skipped",
                fetched=0,
                ingested=0,
                possible_duplicates=0,
                error=None,
                respect_backoff=respect_backoff,
            )

    try:
        payload = load_source_payload(source)
        postings = to_job_postings_from_payload(source.source_id, payload, scanned_at=scanned_at)
        summary = repository.upsert_postings(postings, return_stats=True)
        return repository.record_job_source_scan_result(
            source.source_id,
            scanned_at=scanned_at,
            trigger=trigger,
            status="ok",
            fetched=len(postings),
            ingested=summary.updated,
            possible_duplicates=summary.possible_duplicates,
            error=None,
            respect_backoff=respect_backoff,
        )
    except Exception as exc:
        error_text = str(exc)
        return repository.record_job_source_scan_result(
            source.source_id,
            scanned_at=scanned_at,
            trigger=trigger,
            status="error",
            fetched=0,
            ingested=0,
            possible_duplicates=0,
            error=error_text,
            respect_backoff=respect_backoff,
        )


def create_app(
    *,
    database_path: str | None = None,
    api_key: str | None = None,
    api_tokens: dict[str, list[str] | set[str]] | None = None,
) -> FastAPI:
    resolved_path = database_path or os.getenv("RECOMMENDER_DB_PATH", DEFAULT_DB_PATH)
    resolved_api_key = (api_key or os.getenv("RECOMMENDER_API_KEY", "")).strip() or None
    resolved_token_map: dict[str, set[str]] = {}
    if api_tokens is not None:
        resolved_token_map = {
            token: set(
                normalize_scopes(
                    {str(scope).strip() for scope in scopes if str(scope).strip()}
                )
            )
            for token, scopes in api_tokens.items()
            if token.strip()
        }
    else:
        raw_tokens = os.getenv("RECOMMENDER_API_TOKENS_JSON", "").strip()
        if raw_tokens:
            resolved_token_map = parse_api_tokens(raw_tokens)

    if resolved_api_key:
        resolved_token_map.setdefault(resolved_api_key, set()).add("*")

    repository = RecommenderRepository(database_path=resolved_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await run_in_threadpool(repository.connect)
        app.state.repository = repository
        app.state.auth_token_scopes = resolved_token_map
        app.state.metrics = MetricsStore()
        try:
            yield
        finally:
            await run_in_threadpool(repository.close)

    app = FastAPI(title="OperationBattleship Recommender", version="0.6.0", lifespan=lifespan)

    async def write_audit_event(
        request: Request,
        *,
        action: str,
        scope: str | None,
        status: str,
        message: str | None = None,
        auth_subject: str | None = None,
    ) -> int:
        request_id = getattr(request.state, "request_id", None)
        source_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        return await run_in_threadpool(
            request.app.state.repository.record_audit_event,
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            action=action,
            scope=scope,
            source_ip=source_ip,
            user_agent=user_agent,
            auth_subject=auth_subject,
            status=status,
            message=message,
        )

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        started = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - started) * 1000
            request.app.state.metrics.observe(
                method=request.method,
                path=request.url.path,
                status_code=500,
                duration_ms=duration_ms,
            )
            LOGGER.exception(
                json.dumps(
                    {
                        "event": "request_complete",
                        "request_id": request_id,
                        "method": request.method,
                        "path": request.url.path,
                        "status_code": 500,
                        "duration_ms": round(duration_ms, 3),
                        "error": str(exc),
                    }
                )
            )
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal Server Error", "request_id": request_id},
                headers={"x-request-id": request_id},
            )

        duration_ms = (time.perf_counter() - started) * 1000
        request.app.state.metrics.observe(
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        response.headers["x-request-id"] = request_id
        LOGGER.info(
            json.dumps(
                {
                    "event": "request_complete",
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": round(duration_ms, 3),
                    "source_ip": request.client.host if request.client else None,
                }
            )
        )
        return response

    async def require_scope(
        request: Request,
        *,
        action: str,
        scope: str,
    ) -> str | None:
        token_map: dict[str, set[str]] = request.app.state.auth_token_scopes
        provided = request.headers.get("x-api-key", "")
        has_env_tokens = bool(token_map)
        has_db_tokens = await run_in_threadpool(request.app.state.repository.has_active_api_tokens)
        auth_configured = has_env_tokens or has_db_tokens
        if not auth_configured:
            return None
        if not provided:
            await write_audit_event(
                request,
                action=action,
                scope=scope,
                status="unauthorized",
                message="missing api key",
            )
            raise HTTPException(status_code=401, detail="Unauthorized")

        token_context: TokenAuthContext | None = None
        env_scopes = token_map.get(provided)
        if env_scopes is not None:
            token_context = TokenAuthContext(
                scopes=env_scopes,
                auth_subject=build_auth_subject(provided),
            )
        else:
            token_context = await run_in_threadpool(
                request.app.state.repository.resolve_db_token,
                provided,
            )

        if token_context is None:
            await write_audit_event(
                request,
                action=action,
                scope=scope,
                status="unauthorized",
                message="invalid, expired, or revoked api key",
            )
            raise HTTPException(status_code=401, detail="Unauthorized")

        if token_context.token_id:
            await run_in_threadpool(
                request.app.state.repository.touch_api_token_usage,
                token_context.token_id,
                source_ip=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
            )

        auth_subject = token_context.auth_subject
        scopes = token_context.scopes
        if "*" not in scopes and scope not in scopes:
            await write_audit_event(
                request,
                action=action,
                scope=scope,
                status="forbidden",
                message="missing required scope",
                auth_subject=auth_subject,
            )
            raise HTTPException(status_code=403, detail="Forbidden")
        return auth_subject

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "recommender"}

    @app.get("/metrics", response_model=MetricsSnapshot)
    async def metrics(request: Request) -> MetricsSnapshot:
        return request.app.state.metrics.snapshot()

    @app.post("/postings", response_model=UpsertPostingsResponse)
    async def upsert_postings(
        payload: UpsertPostingsRequest,
        request: Request,
        response: Response,
    ) -> UpsertPostingsResponse:
        auth_subject = await require_scope(
            request,
            action="postings_upsert",
            scope="postings:write",
        )
        updated = await run_in_threadpool(
            request.app.state.repository.upsert_postings,
            payload.postings,
        )
        event_id = await write_audit_event(
            request,
            action="postings_upsert",
            scope="postings:write",
            status="ok",
            message=f"updated={updated}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return UpsertPostingsResponse(updated=updated)

    @app.get("/postings", response_model=list[StoredPosting])
    async def list_postings(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[StoredPosting]:
        return await run_in_threadpool(request.app.state.repository.list_postings, limit)

    @app.post("/job-sources", response_model=JobSource)
    async def upsert_job_source(
        payload: JobSourceUpsertRequest,
        request: Request,
        response: Response,
    ) -> JobSource:
        auth_subject = await require_scope(
            request,
            action="job_source_upsert",
            scope="sources:write",
        )
        source = await run_in_threadpool(request.app.state.repository.upsert_job_source, payload)
        event_id = await write_audit_event(
            request,
            action="job_source_upsert",
            scope="sources:write",
            status="ok",
            message=f"source_id={source.source_id}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return source

    @app.get("/job-sources", response_model=list[JobSource])
    async def list_job_sources(
        request: Request,
        enabled_only: bool = Query(default=False),
    ) -> list[JobSource]:
        return await run_in_threadpool(request.app.state.repository.list_job_sources, enabled_only)

    @app.post("/job-sources/{source_id}/scan", response_model=JobSourceScanResult)
    async def scan_job_source(
        source_id: str,
        request: Request,
        response: Response,
        respect_backoff: bool = Query(default=False),
    ) -> JobSourceScanResult:
        auth_subject = await require_scope(
            request,
            action="job_source_scan_one",
            scope="scan",
        )
        source = await run_in_threadpool(request.app.state.repository.get_job_source, source_id)
        if source is None:
            event_id = await write_audit_event(
                request,
                action="job_source_scan_one",
                scope="scan",
                status="not_found",
                message=f"source_id={source_id}",
                auth_subject=auth_subject,
            )
            response.headers["x-audit-event-id"] = str(event_id)
            raise HTTPException(status_code=404, detail="Unknown source_id")

        result = await run_in_threadpool(
            scan_source,
            request.app.state.repository,
            source,
            trigger="manual",
            respect_backoff=respect_backoff,
        )
        if result.status == "error":
            event_id = await write_audit_event(
                request,
                action="job_source_scan_one",
                scope="scan",
                status="error",
                message=f"source_id={source_id}; error={result.error}",
                auth_subject=auth_subject,
            )
            response.headers["x-audit-event-id"] = str(event_id)
            raise HTTPException(
                status_code=502,
                detail={"source_id": result.source_id, "error": result.error},
            )
        event_id = await write_audit_event(
            request,
            action="job_source_scan_one",
            scope="scan",
            status="ok",
            message=(
                f"source_id={source_id}; status={result.status}; ingested={result.ingested}; "
                f"possible_duplicates={result.possible_duplicates}"
            ),
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return result

    @app.post("/job-sources/scan", response_model=JobSourceScanBatchResponse)
    async def scan_job_sources(
        request: Request,
        response: Response,
        enabled_only: bool = Query(default=True),
        respect_backoff: bool = Query(default=False),
    ) -> JobSourceScanBatchResponse:
        auth_subject = await require_scope(
            request,
            action="job_source_scan_all",
            scope="scan",
        )
        sources = await run_in_threadpool(
            request.app.state.repository.list_scan_targets,
            enabled_only=enabled_only,
            respect_backoff=False,
            now_iso=now_utc_iso(),
        )
        results: list[JobSourceScanResult] = []
        for source in sources:
            result = await run_in_threadpool(
                scan_source,
                request.app.state.repository,
                source,
                trigger="manual",
                respect_backoff=respect_backoff,
            )
            results.append(result)

        batch = JobSourceScanBatchResponse(
            scanned_at=now_utc_iso(),
            trigger="manual",
            respect_backoff=respect_backoff,
            requested_sources=len(sources),
            successful_sources=sum(1 for result in results if result.status == "ok"),
            failed_sources=sum(1 for result in results if result.status == "error"),
            skipped_sources=sum(1 for result in results if result.status == "skipped"),
            total_ingested=sum(result.ingested for result in results),
            possible_duplicates=sum(result.possible_duplicates for result in results),
            results=results,
        )
        event_id = await write_audit_event(
            request,
            action="job_source_scan_all",
            scope="scan",
            status="ok" if batch.failed_sources == 0 else "partial",
            message=(
                f"requested={batch.requested_sources}; success={batch.successful_sources}; "
                f"failed={batch.failed_sources}; skipped={batch.skipped_sources}; "
                f"ingested={batch.total_ingested}"
            ),
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return batch

    @app.post("/job-sources/scan/scheduled", response_model=JobSourceScanBatchResponse)
    async def scheduled_scan_job_sources(
        request: Request,
        response: Response,
        enabled_only: bool = Query(default=True),
    ) -> JobSourceScanBatchResponse:
        auth_subject = await require_scope(
            request,
            action="job_source_scan_scheduled",
            scope="scan",
        )
        sources = await run_in_threadpool(
            request.app.state.repository.list_scan_targets,
            enabled_only=enabled_only,
            respect_backoff=False,
            now_iso=now_utc_iso(),
        )
        results: list[JobSourceScanResult] = []
        for source in sources:
            result = await run_in_threadpool(
                scan_source,
                request.app.state.repository,
                source,
                trigger="scheduled",
                respect_backoff=True,
            )
            results.append(result)

        batch = JobSourceScanBatchResponse(
            scanned_at=now_utc_iso(),
            trigger="scheduled",
            respect_backoff=True,
            requested_sources=len(sources),
            successful_sources=sum(1 for result in results if result.status == "ok"),
            failed_sources=sum(1 for result in results if result.status == "error"),
            skipped_sources=sum(1 for result in results if result.status == "skipped"),
            total_ingested=sum(result.ingested for result in results),
            possible_duplicates=sum(result.possible_duplicates for result in results),
            results=results,
        )
        event_id = await write_audit_event(
            request,
            action="job_source_scan_scheduled",
            scope="scan",
            status="ok" if batch.failed_sources == 0 else "partial",
            message=(
                f"requested={batch.requested_sources}; success={batch.successful_sources}; "
                f"failed={batch.failed_sources}; skipped={batch.skipped_sources}; "
                f"ingested={batch.total_ingested}"
            ),
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return batch

    @app.get("/job-sources/scan-history", response_model=list[JobSourceScanHistoryItem])
    async def job_source_scan_history(
        request: Request,
        response: Response,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0, le=10000),
        source_id: str | None = None,
        trigger: Literal["manual", "scheduled"] | None = None,
        status: Literal["ok", "error", "skipped"] | None = None,
        scanned_after: str | None = None,
        scanned_before: str | None = None,
    ) -> list[JobSourceScanHistoryItem]:
        if scanned_after and parse_iso_datetime(scanned_after) is None:
            raise HTTPException(status_code=422, detail="Invalid scanned_after timestamp")
        if scanned_before and parse_iso_datetime(scanned_before) is None:
            raise HTTPException(status_code=422, detail="Invalid scanned_before timestamp")
        auth_subject = await require_scope(
            request,
            action="job_source_scan_history",
            scope="scan",
        )
        history = await run_in_threadpool(
            request.app.state.repository.list_job_source_scan_history,
            limit=limit,
            offset=offset,
            source_id=source_id,
            trigger=trigger,
            status=status,
            scanned_after=scanned_after,
            scanned_before=scanned_before,
        )
        event_id = await write_audit_event(
            request,
            action="job_source_scan_history",
            scope="scan",
            status="ok",
            message=(
                f"returned={len(history)}; limit={limit}; offset={offset}; "
                f"source_id={source_id or '*'}; trigger={trigger or '*'}; "
                f"status_filter={status or '*'}"
            ),
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return history

    @app.post("/profiles", response_model=UserPreferenceProfile)
    async def upsert_profile(
        payload: UserProfileUpsertRequest,
        request: Request,
        response: Response,
    ) -> UserPreferenceProfile:
        auth_subject = await require_scope(
            request,
            action="profile_upsert",
            scope="profiles:write",
        )
        profile = await run_in_threadpool(request.app.state.repository.upsert_user_profile, payload)
        event_id = await write_audit_event(
            request,
            action="profile_upsert",
            scope="profiles:write",
            status="ok",
            message=f"profile_id={profile.profile_id}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return profile

    @app.get("/profiles", response_model=list[UserPreferenceProfile])
    async def list_profiles(request: Request) -> list[UserPreferenceProfile]:
        return await run_in_threadpool(request.app.state.repository.list_user_profiles)

    @app.get("/profiles/{profile_id}", response_model=UserPreferenceProfile)
    async def get_profile(profile_id: str, request: Request) -> UserPreferenceProfile:
        profile = await run_in_threadpool(request.app.state.repository.get_user_profile, profile_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="Unknown profile_id")
        return profile

    @app.delete("/profiles/{profile_id}")
    async def delete_profile(
        profile_id: str,
        request: Request,
        response: Response,
    ) -> dict[str, bool]:
        auth_subject = await require_scope(
            request,
            action="profile_delete",
            scope="profiles:write",
        )
        deleted = await run_in_threadpool(
            request.app.state.repository.delete_user_profile,
            profile_id,
        )
        if not deleted:
            event_id = await write_audit_event(
                request,
                action="profile_delete",
                scope="profiles:write",
                status="not_found",
                message=f"profile_id={profile_id}",
                auth_subject=auth_subject,
            )
            response.headers["x-audit-event-id"] = str(event_id)
            raise HTTPException(status_code=404, detail="Unknown profile_id")
        event_id = await write_audit_event(
            request,
            action="profile_delete",
            scope="profiles:write",
            status="ok",
            message=f"profile_id={profile_id}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return {"deleted": True}

    @app.post("/auth/tokens", response_model=ApiTokenCreateResponse)
    async def create_token(
        payload: ApiTokenCreateRequest,
        request: Request,
        response: Response,
    ) -> ApiTokenCreateResponse:
        auth_subject = await require_scope(
            request,
            action="token_create",
            scope="tokens:write",
        )
        token = await run_in_threadpool(request.app.state.repository.create_api_token, payload)
        event_id = await write_audit_event(
            request,
            action="token_create",
            scope="tokens:write",
            status="ok",
            message=f"token_id={token.metadata.token_id}; scopes={','.join(token.metadata.scopes)}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return token

    @app.get("/auth/tokens", response_model=list[ApiTokenMetadata])
    async def list_tokens(
        request: Request,
        response: Response,
        include_revoked: bool = Query(default=False),
    ) -> list[ApiTokenMetadata]:
        auth_subject = await require_scope(
            request,
            action="token_list",
            scope="tokens:read",
        )
        tokens = await run_in_threadpool(
            request.app.state.repository.list_api_tokens,
            include_revoked=include_revoked,
        )
        event_id = await write_audit_event(
            request,
            action="token_list",
            scope="tokens:read",
            status="ok",
            message=f"returned={len(tokens)}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return tokens

    @app.post("/auth/tokens/{token_id}/revoke")
    async def revoke_token(
        token_id: str,
        request: Request,
        response: Response,
    ) -> dict[str, bool]:
        auth_subject = await require_scope(
            request,
            action="token_revoke",
            scope="tokens:write",
        )
        revoked = await run_in_threadpool(request.app.state.repository.revoke_api_token, token_id)
        if not revoked:
            event_id = await write_audit_event(
                request,
                action="token_revoke",
                scope="tokens:write",
                status="not_found",
                message=f"token_id={token_id}",
                auth_subject=auth_subject,
            )
            response.headers["x-audit-event-id"] = str(event_id)
            raise HTTPException(status_code=404, detail="Unknown token_id")
        event_id = await write_audit_event(
            request,
            action="token_revoke",
            scope="tokens:write",
            status="ok",
            message=f"token_id={token_id}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return {"revoked": True}

    @app.get("/audit-events", response_model=list[AuditEvent])
    async def list_audit_events(
        request: Request,
        response: Response,
        limit: int = Query(default=100, ge=1, le=500),
        action: str | None = None,
        status: str | None = None,
    ) -> list[AuditEvent]:
        auth_subject = await require_scope(
            request,
            action="audit_events_list",
            scope="audit:read",
        )
        events = await run_in_threadpool(
            request.app.state.repository.list_audit_events,
            limit=limit,
            action=action,
            status=status,
        )
        event_id = await write_audit_event(
            request,
            action="audit_events_list",
            scope="audit:read",
            status="ok",
            message=f"returned={len(events)}",
            auth_subject=auth_subject,
        )
        response.headers["x-audit-event-id"] = str(event_id)
        return events

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
        profile: UserPreferenceProfile | None = None
        if payload.profile_id:
            profile = await run_in_threadpool(
                request.app.state.repository.get_user_profile,
                payload.profile_id,
            )
            if profile is None:
                raise HTTPException(status_code=404, detail="Unknown profile_id")

        postings = payload.postings
        if not postings:
            source = "stored"
            stored_postings = await run_in_threadpool(
                request.app.state.repository.list_postings,
                payload.max_postings,
            )
            postings = [
                JobPosting(
                    id=posting.id,
                    title=posting.title,
                    description=posting.description,
                    company=posting.company,
                    location=posting.location,
                    apply_url=posting.apply_url,
                    source_id=posting.source_id,
                    external_id=posting.external_id,
                    dedup_key=posting.dedup_key,
                    duplicate_hint_count=posting.duplicate_hint_count,
                    updated_at=posting.updated_at,
                )
                for posting in stored_postings
            ]
        else:
            await run_in_threadpool(request.app.state.repository.upsert_postings, postings)

        preferred_keywords, preferred_locations, preferred_companies, remote_only = (
            resolve_recommendation_preferences(payload, profile)
        )

        ranked = rank_postings(
            payload.resume_text,
            postings,
            preferred_keywords=preferred_keywords,
            preferred_locations=preferred_locations,
            preferred_companies=preferred_companies,
            remote_only=remote_only,
        )
        run_id, generated_at = await run_in_threadpool(
            request.app.state.repository.record_recommendations,
            payload.resume_text,
            ranked,
        )
        return RecommendResponse(
            run_id=run_id,
            source=source,
            applied_profile_id=profile.profile_id if profile else None,
            generated_at=generated_at,
            recommendations=ranked,
        )

    return app


app = create_app()

from __future__ import annotations

from common.utils import now_utc_iso, tokenize
from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="OperationBattleship Recommender", version="0.1.0")


class JobPosting(BaseModel):
    id: str = Field(..., description="Unique job identifier")
    title: str
    description: str


class RecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[JobPosting] = Field(default_factory=list)


class RankedRecommendation(BaseModel):
    id: str
    title: str
    score: float


class RecommendResponse(BaseModel):
    generated_at: str
    recommendations: list[RankedRecommendation]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "recommender"}


@app.post("/recommend", response_model=RecommendResponse)
async def recommend(payload: RecommendRequest) -> RecommendResponse:
    resume_tokens = tokenize(payload.resume_text)
    ranked: list[RankedRecommendation] = []

    for posting in payload.postings:
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
    return RecommendResponse(generated_at=now_utc_iso(), recommendations=ranked)

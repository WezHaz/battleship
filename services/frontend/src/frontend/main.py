from __future__ import annotations

import os
from typing import Any

import httpx
from common.utils import now_utc_iso
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI(title="OperationBattleship Frontend", version="0.1.0")
RECOMMENDER_BASE_URL = os.getenv("RECOMMENDER_BASE_URL", "http://localhost:8001")
RECOMMENDER_API_KEY = os.getenv("RECOMMENDER_API_KEY", "").strip()


class UIRecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[str] = Field(default_factory=list)


class UIScanRequest(BaseModel):
    postings: list[str] = Field(default_factory=list, min_length=1)


class UISourceScanRequest(BaseModel):
    enabled_only: bool = True


def build_postings(postings: list[str]) -> list[dict[str, str]]:
    return [
        {"id": f"job-{index + 1}", "title": posting, "description": posting}
        for index, posting in enumerate(postings)
    ]


def recommender_headers() -> dict[str, str]:
    if not RECOMMENDER_API_KEY:
        return {}
    return {"x-api-key": RECOMMENDER_API_KEY}


async def post_to_recommender(path: str, payload: dict[str, Any] | None = None) -> dict:
    request_kwargs: dict[str, Any] = {"headers": recommender_headers()}
    if payload is not None:
        request_kwargs["json"] = payload

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"{RECOMMENDER_BASE_URL}{path}",
                **request_kwargs,
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="Upstream recommender is unavailable") from exc

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Upstream recommender request failed")

    return response.json()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "frontend"}


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"UTF-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
    <title>OperationBattleship</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem; max-width: 900px; }
      textarea, input { width: 100%; margin: 0.5rem 0; padding: 0.6rem; }
      button { padding: 0.6rem 1rem; cursor: pointer; }
      pre { background: #f7f7f7; padding: 1rem; overflow-x: auto; }
    </style>
  </head>
  <body>
    <h1>OperationBattleship</h1>
    <p>Enter resume text and one job posting per line.</p>
    <label>Resume</label>
    <textarea id=\"resume\" rows=\"8\" placeholder=\"Paste resume text\"></textarea>
    <label>Job postings (one per line)</label>
    <textarea id=\"postings\" rows=\"6\" placeholder=\"Backend Engineer\\nML Engineer\"></textarea>
    <button onclick=\"scanConfiguredSources()\">Scan Configured Sources</button>
    <button onclick=\"scanPostings()\">Scan Postings</button>
    <button onclick=\"submitData()\">Get Recommendations</button>
    <button onclick=\"scanAndRecommend()\">Scan + Recommend</button>
    <h2>Response</h2>
    <pre id=\"output\"></pre>

    <script>
      async function callApi(path, payload) {
        const response = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(JSON.stringify(data));
        }
        return data;
      }

      function readPostings() {
        return document.getElementById('postings').value
          .split('\\n')
          .map(s => s.trim())
          .filter(Boolean);
      }

      async function scanPostings() {
        const postings = readPostings();
        const data = await callApi('/api/scan', { postings });
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);
      }

      async function scanConfiguredSources() {
        const data = await callApi('/api/scan/sources', { enabled_only: true });
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);
      }

      async function submitData() {
        const resume = document.getElementById('resume').value;
        const postings = readPostings();
        const data = await callApi('/api/recommend', { resume_text: resume, postings });
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);
      }

      async function scanAndRecommend() {
        const resume = document.getElementById('resume').value;
        const postings = readPostings();
        const scanData = await callApi('/api/scan', { postings });
        const recommendData = await callApi(
          '/api/recommend',
          { resume_text: resume, postings: [] }
        );
        document.getElementById('output').textContent = JSON.stringify({
          scan: scanData,
          recommend: recommendData
        }, null, 2);
      }
    </script>
  </body>
</html>
"""


@app.post("/api/scan")
async def proxy_scan(payload: UIScanRequest) -> dict:
    recommender_payload = {"postings": build_postings(payload.postings)}
    recommender_response = await post_to_recommender("/postings", recommender_payload)
    return {
        "gateway_generated_at": now_utc_iso(),
        "recommender_response": recommender_response,
    }


@app.post("/api/scan/sources")
async def proxy_scan_sources(payload: UISourceScanRequest) -> dict:
    recommender_response = await post_to_recommender(
        f"/job-sources/scan?enabled_only={'true' if payload.enabled_only else 'false'}",
        {},
    )
    return {
        "gateway_generated_at": now_utc_iso(),
        "recommender_response": recommender_response,
    }


@app.post("/api/recommend")
async def proxy_recommend(payload: UIRecommendRequest) -> dict:
    recommender_payload = {
        "resume_text": payload.resume_text,
        "postings": build_postings(payload.postings),
    }
    recommender_response = await post_to_recommender("/recommend", recommender_payload)
    return {
        "gateway_generated_at": now_utc_iso(),
        "recommender_response": recommender_response,
    }

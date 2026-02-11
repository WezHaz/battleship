from __future__ import annotations

import os

import httpx
from common.utils import now_utc_iso
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI(title="OperationBattleship Frontend", version="0.1.0")
RECOMMENDER_BASE_URL = os.getenv("RECOMMENDER_BASE_URL", "http://localhost:8001")


class UIRecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[str] = Field(default_factory=list)


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
    <button onclick=\"submitData()\">Get Recommendations</button>
    <h2>Response</h2>
    <pre id=\"output\"></pre>

    <script>
      async function submitData() {
        const resume = document.getElementById('resume').value;
        const postings = document.getElementById('postings').value
          .split('\n')
          .map(s => s.trim())
          .filter(Boolean);

        const response = await fetch('/api/recommend', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ resume_text: resume, postings })
        });

        const data = await response.json();
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);
      }
    </script>
  </body>
</html>
"""


@app.post("/api/recommend")
async def proxy_recommend(payload: UIRecommendRequest) -> dict:
    recommender_payload = {
        "resume_text": payload.resume_text,
        "postings": [
            {"id": f"job-{index + 1}", "title": posting, "description": posting}
            for index, posting in enumerate(payload.postings)
        ],
    }

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(f"{RECOMMENDER_BASE_URL}/recommend", json=recommender_payload)

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Upstream recommender request failed")

    return {
        "gateway_generated_at": now_utc_iso(),
        "recommender_response": response.json(),
    }

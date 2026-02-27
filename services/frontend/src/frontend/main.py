from __future__ import annotations

import os
from typing import Any

import httpx
from common.utils import now_utc_iso
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI(title="OperationBattleship Frontend", version="0.2.0")
RECOMMENDER_BASE_URL = os.getenv("RECOMMENDER_BASE_URL", "http://localhost:8001")
RECOMMENDER_API_KEY = os.getenv("RECOMMENDER_API_KEY", "").strip()


class UIRecommendRequest(BaseModel):
    resume_text: str = Field(..., min_length=20)
    postings: list[str] = Field(default_factory=list)
    profile_id: str | None = None
    preferred_keywords: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    preferred_companies: list[str] = Field(default_factory=list)
    remote_only: bool | None = None


class UIScanRequest(BaseModel):
    postings: list[str] = Field(default_factory=list, min_length=1)


class UISourceScanRequest(BaseModel):
    enabled_only: bool = True


class UIProfileUpsertRequest(BaseModel):
    profile_id: str = Field(..., min_length=3, max_length=64)
    name: str = Field(..., min_length=1, max_length=120)
    preferred_keywords: list[str] = Field(default_factory=list)
    preferred_locations: list[str] = Field(default_factory=list)
    preferred_companies: list[str] = Field(default_factory=list)
    remote_only: bool = False


def build_postings(postings: list[str]) -> list[dict[str, str]]:
    return [
        {"id": f"job-{index + 1}", "title": posting, "description": posting}
        for index, posting in enumerate(postings)
    ]


def recommender_headers() -> dict[str, str]:
    if not RECOMMENDER_API_KEY:
        return {}
    return {"x-api-key": RECOMMENDER_API_KEY}


def build_gateway_response(response: httpx.Response, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "gateway_generated_at": now_utc_iso(),
        "upstream_request_id": response.headers.get("x-request-id"),
        "upstream_audit_event_id": response.headers.get("x-audit-event-id"),
        "recommender_response": payload,
    }


async def request_to_recommender(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_kwargs: dict[str, Any] = {"headers": recommender_headers()}
    if payload is not None:
        request_kwargs["json"] = payload

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.request(
                method=method,
                url=f"{RECOMMENDER_BASE_URL}{path}",
                **request_kwargs,
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="Upstream recommender is unavailable") from exc

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {}

    if response.status_code >= 400:
        detail = response_payload.get("detail", "Upstream recommender request failed")
        if 400 <= response.status_code < 500:
            raise HTTPException(status_code=response.status_code, detail=detail)
        raise HTTPException(status_code=502, detail=detail)

    return build_gateway_response(response, response_payload)


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
      body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem; max-width: 1100px; }
      h2 { margin-top: 1.5rem; }
      .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
      .panel { border: 1px solid #ddd; border-radius: 8px; padding: 1rem; }
      textarea, input, select { width: 100%; margin: 0.35rem 0; padding: 0.55rem; }
      button { padding: 0.55rem 0.9rem; cursor: pointer; margin-right: 0.4rem; margin-top: 0.4rem; }
      pre { background: #f7f7f7; padding: 1rem; overflow-x: auto; min-height: 160px; }
      @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <h1>OperationBattleship</h1>
    <p>Profile-aware scanning and recommendation UI for operators and agents.</p>

    <div class=\"grid\">
      <div class=\"panel\">
        <h2>Profile</h2>
        <label>Profile ID</label>
        <input id=\"profile_id\" placeholder=\"wesley_remote\" />
        <label>Profile Name</label>
        <input id=\"profile_name\" placeholder=\"Wesley Remote\" />
        <label>Preferred Keywords (comma-separated)</label>
        <input id=\"preferred_keywords\" placeholder=\"python,api,backend\" />
        <label>Preferred Locations (comma-separated)</label>
        <input id=\"preferred_locations\" placeholder=\"remote,austin\" />
        <label>Preferred Companies (comma-separated)</label>
        <input id=\"preferred_companies\" placeholder=\"acme labs\" />
        <label>Remote Preference</label>
        <select id=\"remote_mode\">
          <option value=\"use_profile\">Use Profile/Default</option>
          <option value=\"true\">Remote Only</option>
          <option value=\"false\">Allow Onsite</option>
        </select>
        <button onclick=\"saveProfile()\">Save Profile</button>
        <button onclick=\"loadProfiles()\">Load Profiles</button>
        <button onclick=\"deleteProfile()\">Delete Profile</button>
      </div>

      <div class=\"panel\">
        <h2>Resume + Jobs</h2>
        <label>Resume</label>
        <textarea id=\"resume\" rows=\"8\" placeholder=\"Paste resume text\"></textarea>
        <label>Job postings (one per line)</label>
        <textarea
          id=\"postings\"
          rows=\"8\"
          placeholder=\"Backend Engineer\\nML Engineer\"
        ></textarea>
        <button onclick=\"scanConfiguredSources()\">Scan Configured Sources</button>
        <button onclick=\"scanPostings()\">Scan Postings</button>
        <button onclick=\"submitData()\">Get Recommendations</button>
        <button onclick=\"scanAndRecommend()\">Scan + Recommend</button>
      </div>
    </div>

    <h2>Response</h2>
    <pre id=\"output\"></pre>

    <script>
      function parseCsv(value) {
        return value
          .split(',')
          .map(s => s.trim())
          .filter(Boolean);
      }

      function readPostings() {
        return document.getElementById('postings').value
          .split('\\n')
          .map(s => s.trim())
          .filter(Boolean);
      }

      function readRemoteOnly() {
        const mode = document.getElementById('remote_mode').value;
        if (mode === 'true') return true;
        if (mode === 'false') return false;
        return null;
      }

      async function callApi(path, method, payload = null) {
        const response = await fetch(path, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: payload === null ? null : JSON.stringify(payload)
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(JSON.stringify(data));
        }
        return data;
      }

      function writeOutput(data) {
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);
      }

      function readProfilePayload() {
        return {
          profile_id: document.getElementById('profile_id').value.trim(),
          name: document.getElementById('profile_name').value.trim() || 'Unnamed Profile',
          preferred_keywords: parseCsv(document.getElementById('preferred_keywords').value),
          preferred_locations: parseCsv(document.getElementById('preferred_locations').value),
          preferred_companies: parseCsv(document.getElementById('preferred_companies').value),
          remote_only: readRemoteOnly() === true
        };
      }

      async function saveProfile() {
        const payload = readProfilePayload();
        const data = await callApi('/api/profiles', 'POST', payload);
        writeOutput(data);
      }

      async function loadProfiles() {
        const data = await callApi('/api/profiles', 'GET');
        writeOutput(data);
      }

      async function deleteProfile() {
        const profileId = document.getElementById('profile_id').value.trim();
        if (!profileId) throw new Error('Profile ID is required for delete.');
        const data = await callApi(`/api/profiles/${profileId}`, 'DELETE');
        writeOutput(data);
      }

      async function scanPostings() {
        const postings = readPostings();
        const data = await callApi('/api/scan', 'POST', { postings });
        writeOutput(data);
      }

      async function scanConfiguredSources() {
        const data = await callApi('/api/scan/sources', 'POST', { enabled_only: true });
        writeOutput(data);
      }

      async function submitData() {
        const profileId = document.getElementById('profile_id').value.trim();
        const data = await callApi('/api/recommend', 'POST', {
          resume_text: document.getElementById('resume').value,
          postings: readPostings(),
          profile_id: profileId || null,
          preferred_keywords: parseCsv(document.getElementById('preferred_keywords').value),
          preferred_locations: parseCsv(document.getElementById('preferred_locations').value),
          preferred_companies: parseCsv(document.getElementById('preferred_companies').value),
          remote_only: readRemoteOnly()
        });
        writeOutput(data);
      }

      async function scanAndRecommend() {
        const profileId = document.getElementById('profile_id').value.trim();
        const scanData = await callApi('/api/scan', 'POST', { postings: readPostings() });
        const recommendData = await callApi('/api/recommend', 'POST', {
          resume_text: document.getElementById('resume').value,
          postings: [],
          profile_id: profileId || null,
          preferred_keywords: parseCsv(document.getElementById('preferred_keywords').value),
          preferred_locations: parseCsv(document.getElementById('preferred_locations').value),
          preferred_companies: parseCsv(document.getElementById('preferred_companies').value),
          remote_only: readRemoteOnly()
        });
        writeOutput({ scan: scanData, recommend: recommendData });
      }
    </script>
  </body>
</html>
"""


@app.post("/api/scan")
async def proxy_scan(payload: UIScanRequest) -> dict[str, Any]:
    recommender_payload = {"postings": build_postings(payload.postings)}
    return await request_to_recommender("POST", "/postings", recommender_payload)


@app.post("/api/scan/sources")
async def proxy_scan_sources(payload: UISourceScanRequest) -> dict[str, Any]:
    enabled_only = "true" if payload.enabled_only else "false"
    return await request_to_recommender(
        "POST",
        f"/job-sources/scan?enabled_only={enabled_only}",
        {},
    )


@app.post("/api/profiles")
async def proxy_upsert_profile(payload: UIProfileUpsertRequest) -> dict[str, Any]:
    recommender_payload = {
        "profile_id": payload.profile_id,
        "name": payload.name,
        "preferred_keywords": payload.preferred_keywords,
        "preferred_locations": payload.preferred_locations,
        "preferred_companies": payload.preferred_companies,
        "remote_only": payload.remote_only,
    }
    return await request_to_recommender("POST", "/profiles", recommender_payload)


@app.get("/api/profiles")
async def proxy_list_profiles() -> dict[str, Any]:
    return await request_to_recommender("GET", "/profiles")


@app.delete("/api/profiles/{profile_id}")
async def proxy_delete_profile(profile_id: str) -> dict[str, Any]:
    return await request_to_recommender("DELETE", f"/profiles/{profile_id}")


@app.post("/api/recommend")
async def proxy_recommend(payload: UIRecommendRequest) -> dict[str, Any]:
    recommender_payload = {
        "resume_text": payload.resume_text,
        "postings": build_postings(payload.postings),
        "profile_id": payload.profile_id,
        "preferred_keywords": payload.preferred_keywords,
        "preferred_locations": payload.preferred_locations,
        "preferred_companies": payload.preferred_companies,
        "remote_only": payload.remote_only,
    }
    return await request_to_recommender("POST", "/recommend", recommender_payload)

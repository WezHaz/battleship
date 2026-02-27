from __future__ import annotations

import os
from typing import Any, Literal
from urllib.parse import urlencode

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
    trigger: Literal["manual", "scheduled"] = "manual"
    respect_backoff: bool = False


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

    <div class=\"panel\">
      <h2>Source Scan Panel</h2>
      <label><input type=\"checkbox\" id=\"sources_enabled_only\" /> Enabled Only</label>
      <label>Scan Trigger</label>
      <select id=\"source_scan_trigger\">
        <option value=\"all\">All</option>
        <option value=\"manual\">Manual</option>
        <option value=\"scheduled\">Scheduled</option>
      </select>
      <label><input type=\"checkbox\" id=\"source_scan_backoff\" /> Respect Backoff</label>
      <label>History Source Filter (optional)</label>
      <input id=\"scan_history_source_id\" placeholder=\"source_id\" />
      <label>History Status</label>
      <select id=\"scan_history_status\">
        <option value=\"all\">All</option>
        <option value=\"ok\">OK</option>
        <option value=\"error\">Error</option>
        <option value=\"skipped\">Skipped</option>
      </select>
      <label>History Scanned After (local time, optional)</label>
      <input id=\"scan_history_after\" type=\"datetime-local\" />
      <label>History Scanned Before (local time, optional)</label>
      <input id=\"scan_history_before\" type=\"datetime-local\" />
      <label>History Page Size</label>
      <input id=\"scan_history_limit\" type=\"number\" min=\"1\" max=\"100\" value=\"25\" />
      <button onclick=\"loadSources()\">Load Sources</button>
      <button onclick=\"scanConfiguredSources()\">Scan All Sources</button>
      <button onclick=\"loadScanHistory()\">Load Scan History</button>
      <button onclick=\"scanHistoryPreviousPage()\">History Previous</button>
      <button onclick=\"scanHistoryNextPage()\">History Next</button>
      <div id=\"sources_list\"></div>
      <div id=\"scan_history_pagination\"></div>
      <div id=\"scan_history_list\"></div>
    </div>

    <h2>Response</h2>
    <pre id=\"output\"></pre>

    <script>
      let sourceCache = [];
      let scanHistoryCache = [];
      let scanHistoryOffset = 0;

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

      function readHistoryLimit() {
        const limitInput = document.getElementById('scan_history_limit').value || '25';
        const raw = Number.parseInt(limitInput, 10);
        if (!Number.isFinite(raw)) return 25;
        return Math.min(100, Math.max(1, raw));
      }

      function datetimeLocalToIso(value) {
        if (!value) return null;
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return null;
        return parsed.toISOString();
      }

      function readSourceScanOptions() {
        const enabledOnly = document.getElementById('sources_enabled_only').checked;
        const trigger = document.getElementById('source_scan_trigger').value;
        const respectBackoff = document.getElementById('source_scan_backoff').checked;
        return {
          enabled_only: enabledOnly,
          trigger: trigger === 'all' ? 'manual' : trigger,
          respect_backoff: trigger === 'scheduled' ? true : respectBackoff
        };
      }

      function readScanHistoryFilters() {
        const triggerValue = document.getElementById('source_scan_trigger').value;
        const statusValue = document.getElementById('scan_history_status').value;
        return {
          source_id: document.getElementById('scan_history_source_id').value.trim() || null,
          trigger: triggerValue === 'all' ? null : triggerValue,
          status: statusValue === 'all' ? null : statusValue,
          scanned_after: datetimeLocalToIso(document.getElementById('scan_history_after').value),
          scanned_before: datetimeLocalToIso(document.getElementById('scan_history_before').value),
          limit: readHistoryLimit()
        };
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

      function escapeHtml(value) {
        return value
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;')
          .replaceAll('"', '&quot;')
          .replaceAll("'", '&#39;');
      }

      function renderSources() {
        const container = document.getElementById('sources_list');
        if (!sourceCache.length) {
          container.innerHTML = '<p>No sources found.</p>';
          return;
        }

        const rows = sourceCache.map(source => {
          const sourceId = escapeHtml(source.source_id);
          const name = escapeHtml(source.name || source.source_id);
          const status = escapeHtml(source.last_status || 'unknown');
          const enabled = source.enabled ? 'yes' : 'no';
          const failures = Number.isInteger(source.consecutive_failures)
            ? String(source.consecutive_failures)
            : '0';
          const nextEligible = escapeHtml(source.next_eligible_scan_at || '-');
          const error = escapeHtml(source.last_error || '');
          return `
            <tr>
              <td>${sourceId}</td>
              <td>${name}</td>
              <td>${enabled}</td>
              <td>${status}</td>
              <td>${failures}</td>
              <td>${nextEligible}</td>
              <td>${error}</td>
              <td><button onclick="scanOneSource('${sourceId}')">Scan</button></td>
            </tr>
          `;
        }).join('');

        container.innerHTML = `
          <table style="width:100%; border-collapse:collapse; margin-top:0.6rem;">
            <thead>
              <tr>
                <th align="left">Source ID</th>
                <th align="left">Name</th>
                <th align="left">Enabled</th>
                <th align="left">Last Status</th>
                <th align="left">Failures</th>
                <th align="left">Next Eligible Scan</th>
                <th align="left">Last Error</th>
                <th align="left">Action</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        `;
      }

      function renderScanHistory() {
        const container = document.getElementById('scan_history_list');
        const pageInfo = document.getElementById('scan_history_pagination');
        const limit = readHistoryLimit();
        const pageNumber = Math.floor(scanHistoryOffset / limit) + 1;
        const hasNext = scanHistoryCache.length === limit;
        const pageMeta = `(offset=${scanHistoryOffset}, items=${scanHistoryCache.length})`;

        pageInfo.textContent = `History page ${pageNumber} ${pageMeta}`;

        if (!scanHistoryCache.length) {
          container.innerHTML = '<p>No scan history rows found for current filters.</p>';
          return;
        }

        const rows = scanHistoryCache.map(item => {
          const sourceId = escapeHtml(item.source_id || '');
          const scannedAt = escapeHtml(item.scanned_at || '');
          const trigger = escapeHtml(item.trigger || '');
          const status = escapeHtml(item.status || '');
          const ingested = Number(item.ingested || 0);
          const fetched = Number(item.fetched || 0);
          const duplicates = Number(item.possible_duplicates || 0);
          const attempt = Number(item.attempt_number || 0);
          const backoff = Number(item.backoff_seconds || 0);
          const error = escapeHtml(item.error || '');
          return `
            <tr>
              <td>${scannedAt}</td>
              <td>${sourceId}</td>
              <td>${trigger}</td>
              <td>${status}</td>
              <td>${fetched}</td>
              <td>${ingested}</td>
              <td>${duplicates}</td>
              <td>${attempt}</td>
              <td>${backoff}</td>
              <td>${error}</td>
            </tr>
          `;
        }).join('');

        container.innerHTML = `
          <table style="width:100%; border-collapse:collapse; margin-top:0.6rem;">
            <thead>
              <tr>
                <th align="left">Scanned At</th>
                <th align="left">Source</th>
                <th align="left">Trigger</th>
                <th align="left">Status</th>
                <th align="left">Fetched</th>
                <th align="left">Ingested</th>
                <th align="left">Duplicates</th>
                <th align="left">Attempt</th>
                <th align="left">Backoff (s)</th>
                <th align="left">Error</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
          <p>${hasNext ? 'More history is available.' : 'End of filtered history.'}</p>
        `;
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

      async function loadSources() {
        const enabledOnly = document.getElementById('sources_enabled_only').checked;
        const query = enabledOnly ? 'true' : 'false';
        const data = await callApi(`/api/sources?enabled_only=${query}`, 'GET');
        sourceCache = data.recommender_response || [];
        renderSources();
        writeOutput(data);
      }

      async function scanOneSource(sourceId) {
        const options = readSourceScanOptions();
        const backoff = options.respect_backoff ? 'true' : 'false';
        const data = await callApi(
          `/api/scan/sources/${sourceId}?respect_backoff=${backoff}`,
          'POST',
          {}
        );
        writeOutput(data);
        await loadSources();
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
        const data = await callApi('/api/scan/sources', 'POST', readSourceScanOptions());
        writeOutput(data);
        await loadSources();
      }

      async function loadScanHistory(resetOffset = true) {
        if (resetOffset) {
          scanHistoryOffset = 0;
        }
        const filters = readScanHistoryFilters();
        const params = new URLSearchParams();
        params.set('limit', String(filters.limit));
        params.set('offset', String(scanHistoryOffset));
        if (filters.source_id) params.set('source_id', filters.source_id);
        if (filters.trigger) params.set('trigger', filters.trigger);
        if (filters.status) params.set('status', filters.status);
        if (filters.scanned_after) params.set('scanned_after', filters.scanned_after);
        if (filters.scanned_before) params.set('scanned_before', filters.scanned_before);
        const data = await callApi(`/api/scan/history?${params.toString()}`, 'GET');
        scanHistoryCache = data.recommender_response || [];
        renderScanHistory();
        writeOutput(data);
      }

      async function scanHistoryNextPage() {
        const limit = readHistoryLimit();
        if (scanHistoryCache.length < limit) return;
        scanHistoryOffset += limit;
        await loadScanHistory(false);
      }

      async function scanHistoryPreviousPage() {
        const limit = readHistoryLimit();
        scanHistoryOffset = Math.max(0, scanHistoryOffset - limit);
        await loadScanHistory(false);
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
        await loadSources();
      }

      loadSources();
      loadScanHistory();
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
    respect_backoff = "true" if payload.respect_backoff else "false"
    if payload.trigger == "scheduled":
        return await request_to_recommender(
            "POST",
            f"/job-sources/scan/scheduled?enabled_only={enabled_only}",
            {},
        )
    return await request_to_recommender(
        "POST",
        (
            "/job-sources/scan"
            f"?enabled_only={enabled_only}&respect_backoff={respect_backoff}"
        ),
        {},
    )


@app.get("/api/sources")
async def proxy_list_sources(enabled_only: bool = False) -> dict[str, Any]:
    bool_value = "true" if enabled_only else "false"
    return await request_to_recommender("GET", f"/job-sources?enabled_only={bool_value}")


@app.post("/api/scan/sources/{source_id}")
async def proxy_scan_one_source(
    source_id: str,
    respect_backoff: bool = False,
) -> dict[str, Any]:
    backoff_value = "true" if respect_backoff else "false"
    return await request_to_recommender(
        "POST",
        f"/job-sources/{source_id}/scan?respect_backoff={backoff_value}",
        {},
    )


@app.get("/api/scan/history")
async def proxy_scan_history(
    limit: int = 50,
    offset: int = 0,
    source_id: str | None = None,
    trigger: Literal["manual", "scheduled"] | None = None,
    status: Literal["ok", "error", "skipped"] | None = None,
    scanned_after: str | None = None,
    scanned_before: str | None = None,
) -> dict[str, Any]:
    query_params: dict[str, Any] = {"limit": limit, "offset": offset}
    if source_id:
        query_params["source_id"] = source_id
    if trigger:
        query_params["trigger"] = trigger
    if status:
        query_params["status"] = status
    if scanned_after:
        query_params["scanned_after"] = scanned_after
    if scanned_before:
        query_params["scanned_before"] = scanned_before
    query = urlencode(query_params)
    return await request_to_recommender("GET", f"/job-sources/scan-history?{query}")


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

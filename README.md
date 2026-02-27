# OperationBattleship Monorepo

OperationBattleship is a Python microservices job-search platform scaffold using FastAPI, `uv`, Docker Compose, and Terraform.

## Repository structure

```text
.
├── .github/workflows/
│   ├── ci.yml
│   └── release.yml
├── infra/
│   ├── environments/
│   │   ├── dev/
│   │   └── prod/
│   ├── modules/
│   │   ├── ecs/
│   │   ├── iam/
│   │   ├── rds/
│   │   ├── s3/
│   │   └── vpc/
│   └── README.md
├── libs/
│   └── common/
│       ├── pyproject.toml
│       └── src/common/
├── services/
│   ├── recommender/
│   ├── frontend/
│   └── emailer/
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

## Services

- `recommender`: FastAPI recommendation API
  - `GET /health`
  - `POST /postings`
  - `GET /postings`
  - `POST /job-sources`
  - `GET /job-sources`
  - `POST /job-sources/{source_id}/scan`
  - `POST /job-sources/scan`
  - `POST /recommend`
  - `GET /recommendations/history`
- `frontend`: FastAPI gateway + simple UI
  - `GET /`
  - `POST /api/scan`
  - `POST /api/scan/sources`
  - `POST /api/recommend`
- `emailer`: FastAPI async worker trigger API
  - `GET /health`
  - `POST /cron/digest`

Each service exposes OpenAPI docs at `/docs` and OpenAPI schema at `/openapi.json`.

## Dependency management with uv

This repo uses a `uv` workspace rooted at `pyproject.toml`.

### Bootstrap

```bash
./scripts/bootstrap.sh
```

### Common commands

```bash
# Install workspace dependencies
uv sync --all-packages --group dev

# Refresh lockfile
uv lock

# Lint and test
uv run ruff check .
uv run pytest
```

## Testing Strategy (pytest + pytest-bdd)

The repository uses a single `pytest` runner across three test layers:

- `unit`: fast, isolated tests for pure helpers and local classes
- `integration`: API-level tests with FastAPI `TestClient`
- `bdd`: behavior specs in `.feature` files that double as executable usage examples

BDD feature files live under `tests/bdd/features/`, and step definitions live in `tests/bdd/test_*.py`.

### Run specific suites

```bash
# All tests
uv run pytest

# Minimal readiness harness
uv run pytest -m smoke

# Only unit tests
uv run pytest -m unit

# Only integration tests
uv run pytest -m integration

# Only BDD scenarios
uv run pytest -m bdd
```

## Run locally

```bash
# Full stack via Docker
make dev

# Or run services directly with uv
make run-recommender
make run-frontend
make run-emailer
```

Local URLs:

- Frontend UI: `http://localhost:8000`
- Recommender docs: `http://localhost:8001/docs`
- Emailer docs: `http://localhost:8002/docs`

## Recommender persistence flow

The recommender now persists scanned postings and recommendation history in SQLite.
When running with Docker, the DB file is stored in the `recommender-data` volume.
Each posting is normalized (title/company/location/url) and assigned a `dedup_key`.

Dedup behavior is intentionally light:
- if `source_id + external_id` is present, updates are applied to that same record
- if external IDs are missing, scans keep separate rows (duplicates remain visible)
- possible duplicates are surfaced via `duplicate_hint_count` instead of hard deletion

```bash
# 1) Scan/store job postings
curl -X POST http://localhost:8001/postings \
  -H "Content-Type: application/json" \
  -d '{
    "postings": [
      {"id":"job-1","title":"Backend Engineer","description":"Build Python APIs"},
      {"id":"job-2","title":"Data Engineer","description":"Build ETL pipelines"}
    ]
  }'

# 2) Recommend against stored postings (empty postings list => use DB)
curl -X POST http://localhost:8001/recommend \
  -H "Content-Type: application/json" \
  -d '{
    "resume_text":"Python backend engineer building API systems",
    "postings":[]
  }'

# 3) Inspect history
curl http://localhost:8001/recommendations/history
```

Use the helper scanner script to ingest posting files quickly:

```bash
./scripts/scan_postings.sh ./scripts/example_postings.json
```

The frontend UI now supports three actions in one screen:
- `Scan Postings` -> stores postings in recommender persistence
- `Scan Configured Sources` -> runs all enabled `job_sources`
- `Get Recommendations` -> recommends from the current textarea postings
- `Scan + Recommend` -> stores postings, then recommends using stored postings

## Job sources ingestion layer (automated scanning)

Register a source, then run source scans on demand or from cron.

```bash
# Register inline source from a local JSON file
./scripts/register_job_source.sh demo_local "Demo Local Source" ./scripts/example_postings.json

# Register remote source from URL
./scripts/register_job_source.sh remote_board "Remote Board" https://example.com/postings.json

# Scan all enabled sources
./scripts/scan_sources.sh
```

When a source scan runs:
- postings are normalized and upserted into SQLite
- source health is tracked (`last_scan_at`, `last_status`, `last_error`)
- recommend calls can use stored postings with `"postings":[]`
- source scan results also report `possible_duplicates`

## Recommendation personalization

`POST /recommend` accepts optional preference fields:
- `preferred_keywords`
- `preferred_locations`
- `preferred_companies`
- `remote_only`

Response recommendations now include:
- `matched_terms`
- `score_breakdown` (title/description overlap, preference bonus, freshness bonus, duplicate penalty)

## Security hardening (current)

Set `RECOMMENDER_API_KEY` to protect write endpoints:
- `POST /postings`
- `POST /job-sources`
- `POST /job-sources/{source_id}/scan`
- `POST /job-sources/scan`

When enabled, send the key in `x-api-key`.  
The frontend forwards this header automatically when `RECOMMENDER_API_KEY` is configured for the frontend service too.

## Terraform IaC

Terraform templates are under `infra/` with reusable modules and `dev`/`prod` environments.

```bash
make tf-init-dev
make tf-plan-dev
# make tf-apply-dev
```

Modules provision baseline AWS components:

- VPC + public/private subnets
- ECS cluster + CloudWatch logs
- RDS PostgreSQL
- S3 bucket
- IAM ECS task execution role

## CI/CD workflows

- `ci.yml`: sync deps, lint, test, and build Docker images
- `release.yml`: build/push images and apply Terraform to selected environment

## Notes

- Replace placeholder secrets in `infra/environments/*/terraform.tfvars` with secure secret management.
- Extend service internals (queue provider, persistence, auth) as implementation proceeds.

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
  - `POST /recommend`
  - `GET /recommendations/history`
- `frontend`: FastAPI gateway + simple UI
  - `GET /`
  - `POST /api/scan`
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
- `Get Recommendations` -> recommends from the current textarea postings
- `Scan + Recommend` -> stores postings, then recommends using stored postings

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

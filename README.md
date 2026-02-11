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
  - `POST /recommend`
- `frontend`: FastAPI gateway + simple UI
  - `GET /`
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

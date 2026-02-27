SHELL := /bin/bash

.PHONY: init lock sync lint test test-smoke test-unit test-integration test-bdd dev down \
	 run-recommender run-frontend run-emailer \
	 register-source scan-sources \
	 tf-init-dev tf-plan-dev tf-apply-dev \
	 tf-init-prod tf-plan-prod tf-apply-prod

init:
	uv sync --all-packages --group dev

lock:
	uv lock

sync:
	uv sync --all-packages --group dev

lint:
	uv run ruff check .

test:
	uv run pytest

test-smoke:
	uv run pytest -m smoke

test-unit:
	uv run pytest -m unit

test-integration:
	uv run pytest -m integration

test-bdd:
	uv run pytest -m bdd

dev:
	docker compose up --build

down:
	docker compose down -v

run-recommender:
	uv run --package recommender-service uvicorn recommender.main:app --reload --host 0.0.0.0 --port 8001

run-frontend:
	uv run --package frontend-service uvicorn frontend.main:app --reload --host 0.0.0.0 --port 8000

run-emailer:
	uv run --package emailer-service uvicorn emailer.main:app --reload --host 0.0.0.0 --port 8002

register-source:
	@echo "Usage: ./scripts/register_job_source.sh <source_id> <source_name> <source_input>"

scan-sources:
	./scripts/scan_sources.sh

tf-init-dev:
	terraform -chdir=infra/environments/dev init

tf-plan-dev:
	terraform -chdir=infra/environments/dev plan

tf-apply-dev:
	terraform -chdir=infra/environments/dev apply

tf-init-prod:
	terraform -chdir=infra/environments/prod init

tf-plan-prod:
	terraform -chdir=infra/environments/prod plan

tf-apply-prod:
	terraform -chdir=infra/environments/prod apply

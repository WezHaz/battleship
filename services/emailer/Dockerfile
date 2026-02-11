FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY libs/common libs/common
COPY services services

RUN uv sync --package emailer-service --no-dev

EXPOSE 8002
CMD ["uv", "run", "--package", "emailer-service", "uvicorn", "emailer.main:app", "--host", "0.0.0.0", "--port", "8002"]

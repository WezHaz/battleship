FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY libs/common libs/common
COPY services services

RUN uv sync --package recommender-service --no-dev

EXPOSE 8001
CMD ["uv", "run", "--package", "recommender-service", "uvicorn", "recommender.main:app", "--host", "0.0.0.0", "--port", "8001"]

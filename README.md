# Issue Intelligence Platform

AI-powered platform for software issue triage and resolution assistance.

This repository is the engineering foundation for a production-like
ML/AI system that will process software bug reports, detect duplicates,
retrieve similar resolved issues, and assist engineers with issue resolution.

## Current functionality

The current version provides:

- asynchronous FastAPI application;
- application liveness endpoint;
- application version endpoint;
- end-to-end PostgreSQL health check;
- PostgreSQL integration through async SQLAlchemy and asyncpg;
- centralized configuration;
- structured JSON logging;
- automated tests and coverage;
- Ruff linting and formatting;
- pre-commit checks;
- Docker image;
- Docker Compose environment with PostgreSQL;
- CI pipeline;
- CD pipeline for publishing versioned Docker images to GHCR.

## API

### `GET /healthz`

Fast liveness check of the application itself.

Example:

```json
{
  "status": "ok"
}

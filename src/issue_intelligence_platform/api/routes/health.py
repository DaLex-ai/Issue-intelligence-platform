import logging
from time import perf_counter

from asyncpg.exceptions import PostgresError
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from issue_intelligence_platform.db.database import engine

router = APIRouter(tags=["health"])
logger = logging.getLogger("issue_intelligence.health")


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/v1/health")
async def health() -> JSONResponse:
    start = perf_counter()

    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SHOW server_version"))
            database_version = result.scalar_one()

        response_time_ms = round((perf_counter() - start) * 1000, 2)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "ok",
                "components": {
                    "postgresql": {
                        "status": "ok",
                        "version": database_version,
                        "response_time_ms": response_time_ms,
                    }
                },
            },
        )

    except (SQLAlchemyError, OSError, PostgresError) as exc:
        logger.warning(
            "dependency_unavailable",
            extra={
                "component": "postgresql",
                "error_type": type(exc).__name__,
            },
        )
        response_time_ms = round((perf_counter() - start) * 1000, 2)

        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "components": {
                    "postgresql": {
                        "status": "unavailable",
                        "version": None,
                        "response_time_ms": response_time_ms,
                    }
                },
            },
        )

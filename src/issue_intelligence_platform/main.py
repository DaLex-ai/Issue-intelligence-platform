import logging
from time import perf_counter

from fastapi import FastAPI, Request

from issue_intelligence_platform import __version__
from issue_intelligence_platform.api.routes.health import router as health_router
from issue_intelligence_platform.api.routes.version import router as version_router
from issue_intelligence_platform.core.logging import configure_logging

configure_logging()

logger = logging.getLogger("issue_intelligence.http")

app = FastAPI(
    title="Issue Intelligence Platform",
    version=__version__,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        duration_ms = round((perf_counter() - start) * 1000, 2)

        logger.exception(
            "request_failed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": 500,
                "duration_ms": duration_ms,
            },
        )
        raise

    duration_ms = round((perf_counter() - start) * 1000, 2)

    logger.info(
        "request_completed",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )

    return response


app.include_router(health_router)
app.include_router(version_router)

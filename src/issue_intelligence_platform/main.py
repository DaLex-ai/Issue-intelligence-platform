import asyncio
import logging
from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, Request

from issue_intelligence_platform import __version__
from issue_intelligence_platform.api.routes.health import router as health_router
from issue_intelligence_platform.api.routes.process import router as process_router
from issue_intelligence_platform.api.routes.version import router as version_router
from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.core.logging import configure_logging
from issue_intelligence_platform.db.database import engine, initialize_schema
from issue_intelligence_platform.pipeline.rabbit import ConfirmingPublisher

configure_logging()
logger = logging.getLogger("issue_intelligence.http")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_schema()
    publisher = ConfirmingPublisher()
    await publisher.start()
    app.state.publisher = publisher
    app.state.admission_slots = asyncio.Queue(maxsize=settings.api_publish_slots)
    for _ in range(settings.api_publish_slots):
        app.state.admission_slots.put_nowait(None)
    try:
        yield
    finally:
        await publisher.close()
        await engine.dispose()


app = FastAPI(
    title="Issue Intelligence Platform",
    version=__version__,
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "request_failed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": 500,
                "duration_ms": round((perf_counter() - start) * 1000, 2),
            },
        )
        raise
    logger.info(
        "request_completed",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": round((perf_counter() - start) * 1000, 2),
        },
    )
    return response


app.include_router(health_router)
app.include_router(version_router)
app.include_router(process_router)

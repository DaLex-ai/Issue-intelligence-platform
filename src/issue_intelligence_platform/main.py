import asyncio
import logging
import re
from contextlib import asynccontextmanager, suppress
from time import perf_counter

from fastapi import FastAPI, Request
from opentelemetry.trace import SpanKind

from issue_intelligence_platform import __version__
from issue_intelligence_platform.api.routes.health import router as health_router
from issue_intelligence_platform.api.routes.process import router as process_router
from issue_intelligence_platform.api.routes.version import router as version_router
from issue_intelligence_platform.core import telemetry as obs
from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.core.logging import configure_logging
from issue_intelligence_platform.db.database import (
    engine,
    initialize_schema,
    session_factory,
)
from issue_intelligence_platform.pipeline.rabbit import ConfirmingPublisher
from issue_intelligence_platform.pipeline.reconciliation import reconcile_loop
from issue_intelligence_platform.pipeline.store import TaskStore

obs.setup("app")
configure_logging()
logger = logging.getLogger("issue_intelligence.http")


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = ConfirmingPublisher()
    try:
        await initialize_schema()
        await publisher.start()
    except BaseException:
        logger.exception("api_startup_failed")
        try:
            await publisher.close()
        finally:
            await engine.dispose()
        raise
    reconciler = asyncio.create_task(reconcile_loop(TaskStore(session_factory)))
    app.state.publisher = publisher
    app.state.admission_slots = asyncio.Queue(maxsize=settings.api_publish_slots)
    for _ in range(settings.api_publish_slots):
        app.state.admission_slots.put_nowait(None)
    try:
        yield
    finally:
        logger.info("graceful_shutdown_started")
        reconciler.cancel()
        with suppress(asyncio.CancelledError):
            await reconciler
        try:
            await publisher.close()
        finally:
            await engine.dispose()
        logger.info("graceful_shutdown_completed")


app = FastAPI(
    title="Issue Intelligence Platform",
    version=__version__,
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path
    if path in {"/readyz", "/healthz", "/api/v1/health"}:
        return await call_next(request)
    if re.fullmatch(r"/api/v1/tasks/[^/]+", path):
        route = "/api/v1/tasks/{task_id}"
    elif path in {"/api/v1/tasks", "/process", "/api/v1/version"}:
        route = path
    else:
        route = "other"
    method = request.method if request.method in {"GET", "POST"} else "other"
    start = perf_counter()
    code = 500
    with obs.span(f"{method} {route}", kind=SpanKind.SERVER) as current:
        current.set_attribute("http.route", route)
        current.set_attribute("http.request.method", method)
        try:
            response = await call_next(request)
            code = response.status_code
            if code >= 500:
                obs.fail(f"HTTP {code}")
            return response
        except Exception as exc:
            obs.fail(exc)
            logger.exception("request_failed")
            raise
        finally:
            elapsed = perf_counter() - start
            current.set_attribute("http.response.status_code", code)
            obs.count("iip_api_requests", route=route, code=str(code))
            obs.observe("iip_api_duration_seconds", elapsed, route=route)
            with obs.task_scope(getattr(request.state, "task_id", None)):
                logger.log(
                    logging.WARNING if code >= 500 else logging.INFO,
                    "request_completed",
                    extra={
                        "method": method,
                        "path": route,
                        "status_code": code,
                        "duration_ms": round(elapsed * 1000, 2),
                    },
                )


app.include_router(health_router)
app.include_router(version_router)
app.include_router(process_router)

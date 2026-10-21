"""Asynchronous task admission; legacy /process is not a synchronous inference API."""

import asyncio
from math import ceil
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.db.database import session_factory
from issue_intelligence_platform.pipeline.admission import submit
from issue_intelligence_platform.pipeline.rabbit import PublishUncertain, QueueFull
from issue_intelligence_platform.pipeline.schemas import TaskCreate
from issue_intelligence_platform.pipeline.store import TaskStore

router = APIRouter(tags=["tasks"])


def overloaded_response() -> JSONResponse:
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": str(retry_after_seconds())},
        content={"detail": "Inference capacity unavailable; retry later"},
    )


def retry_after_seconds() -> int:
    # Conservative estimate to clear a FULL queue with one deployed worker:
    # queue depth / (worker_concurrency / duration per inference).
    # Override only when measured workload/backlog warrants it.
    if settings.api_retry_after_seconds > 0:
        return settings.api_retry_after_seconds
    return max(
        1,
        ceil(
            settings.rabbit_max_length
            * max(settings.inference_delay_seconds, 0.01)
            / max(settings.worker_concurrency, 1)
        ),
    )


store = TaskStore(session_factory)


@router.post("/api/v1/tasks", status_code=status.HTTP_202_ACCEPTED)
async def create_task(payload: TaskCreate, request: Request):
    # Fast shedding before touching SQL/AMQP also bounds HTTP publish futures.
    slots = request.app.state.admission_slots
    try:
        slots.get_nowait()
    except asyncio.QueueEmpty:
        return overloaded_response()
    task_id = uuid4()
    try:
        try:
            await submit(
                store,
                request.app.state.publisher,
                task_id,
                payload.summary,
                payload.description,
            )
        except (QueueFull, PublishUncertain):
            return overloaded_response()
    finally:
        slots.put_nowait(None)
    return JSONResponse(
        status_code=202,
        headers={"Location": f"/api/v1/tasks/{task_id}"},
        content={
            "task_id": str(task_id),
            "status": "PENDING",
            "status_url": f"/api/v1/tasks/{task_id}",
        },
    )


@router.get("/api/v1/tasks/{task_id}")
async def get_task(task_id: UUID):
    record = await store.get(task_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return record


@router.post("/process", include_in_schema=False)
async def legacy_process(payload: TaskCreate, request: Request):
    """Existing clients must migrate from synchronous 200 to asynchronous 202."""
    return await create_task(payload, request)

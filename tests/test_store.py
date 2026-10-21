"""Real SQL state transitions exercised against in-memory SQLite."""

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from issue_intelligence_platform.pipeline.store import Base, TaskState, TaskStore


@pytest.fixture
async def store():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield TaskStore(async_sessionmaker(engine, expire_on_commit=False))
    await engine.dispose()


async def test_rejected_task_never_remains_pending(store):
    task_id = uuid4()
    assert await store.stage(task_id, "summary", "description")
    assert (await store.get(task_id))["status"] == TaskState.PUBLISHING
    assert await store.rejected(task_id, "QueueFull")
    assert (await store.get(task_id))["status"] == TaskState.REJECTED
    assert await store.claim(task_id, 3) == "terminal"


async def test_duplicate_delivery_does_not_insert_duplicate_result(store):
    task_id = uuid4()
    await store.stage(task_id, "summary", "desc")
    await store.published(task_id)
    assert await store.claim(task_id, 3) == "claimed"
    assert await store.claim(task_id, 3) == "busy"
    assert await store.succeed(task_id, "High", "model", "champion", "1", 0.1)
    assert not await store.succeed(task_id, "Low", "model", "champion", "1", 0.1)
    assert await store.claim(task_id, 3) == "terminal"
    assert (await store.get(task_id))["prediction"] == "High"


async def test_redelivery_recovers_after_crash_with_finite_attempts(store):
    task_id = uuid4()
    await store.stage(task_id, "summary", "desc")
    await store.published(task_id)
    assert await store.claim(task_id, 2) == "claimed"
    await store.recover(task_id)
    assert await store.claim(task_id, 2) == "claimed"
    assert await store.fail(task_id, "poison", 2)
    assert await store.claim(task_id, 2) == "terminal"
    assert (await store.get(task_id))["attempts"] == 2


async def test_cancel_during_inference_prevents_result(store):
    task_id = uuid4()
    await store.stage(task_id, "summary", "desc")
    assert await store.claim(task_id, 3) == "claimed"
    assert await store.rejected(task_id, "publish uncertain")
    assert not await store.succeed(task_id, "High", "model", "champion", "1", 0.1)
    assert (await store.get(task_id))["status"] == TaskState.REJECTED

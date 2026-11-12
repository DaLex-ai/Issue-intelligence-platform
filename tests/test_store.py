"""State-machine regressions. PostgreSQL concurrency is tested separately."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from issue_intelligence_platform.pipeline.store import (
    Base,
    PublicationInProgress,
    Task,
    TaskState,
    TaskStore,
)


@pytest.fixture
async def store():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield TaskStore(async_sessionmaker(engine, expire_on_commit=False))
    await engine.dispose()


async def accepted(store):
    task_id = uuid4()
    token = await store.stage(task_id, "summary", "desc")
    assert await store.published(task_id, publication_token=token)
    return task_id


async def age(store, task_id):
    async with store.sessions.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == str(task_id))
            .values(updated_at=datetime.now(UTC) - timedelta(seconds=60))
        )


async def test_rejected_task_never_remains_pending(store):
    task_id = uuid4()
    token = await store.stage(task_id, "summary", "desc")
    assert await store.rejected(task_id, "QueueFull", publication_token=token)
    assert (await store.get(task_id))["status"] == TaskState.REJECTED
    assert (await store.claim(task_id, 3)).decision == "terminal"


async def test_duplicate_delivery_does_not_insert_duplicate_result(store):
    task_id = await accepted(store)
    claim = await store.claim(task_id, 3)
    assert claim.decision == "claimed"
    assert (await store.claim(task_id, 3)).decision == "busy"
    assert await store.succeed(
        task_id, "High", "m", "champion", "1", 0.1, attempt=claim.attempt
    )
    assert not await store.succeed(
        task_id, "Low", "m", "champion", "1", 0.1, attempt=claim.attempt
    )
    assert (await store.claim(task_id, 3)).decision == "terminal"
    assert (await store.get(task_id))["prediction"] == "High"


async def test_active_redelivery_cannot_steal_live_lease(store):
    task_id = await accepted(store)
    await store.claim(task_id, 3)
    assert (await store.claim(task_id, 3, redelivered=True)).decision == "busy"
    assert (await store.get(task_id))["attempts"] == 1


async def test_stale_owner_cannot_complete_or_fail_new_attempt(store):
    task_id = await accepted(store)
    old = await store.claim(task_id, 3)
    await age(store, task_id)
    new = await store.claim(task_id, 3, redelivered=True)
    assert new.attempt == old.attempt + 1
    assert not await store.renew(task_id, old.attempt)
    assert not await store.succeed(
        task_id, "old", "m", "champion", "1", 0.1, attempt=old.attempt
    )
    assert not await store.fail(task_id, "old", 3, attempt=old.attempt)
    assert (await store.get(task_id))["status"] == TaskState.PROCESSING
    assert await store.succeed(
        task_id, "new", "m", "champion", "2", 0.1, attempt=new.attempt
    )


async def test_failed_redelivery_remains_failed_for_dlq(store):
    task_id = await accepted(store)
    claim = await store.claim(task_id, 1)
    assert await store.fail(task_id, "poison", 1, attempt=claim.attempt)
    assert (await store.claim(task_id, 1, redelivered=True)).decision == "failed"


async def test_crash_on_last_attempt_becomes_failed(store):
    task_id = await accepted(store)
    await store.claim(task_id, 1)
    await age(store, task_id)
    assert (await store.claim(task_id, 1, redelivered=True)).decision == "failed"


async def test_heartbeat_protects_live_claim(store):
    task_id = await accepted(store)
    claim = await store.claim(task_id, 3)
    await age(store, task_id)
    assert await store.renew(task_id, claim.attempt)
    assert (await store.claim(task_id, 3, redelivered=True)).decision == "busy"


async def test_publishing_message_cannot_run_before_admission(store):
    task_id = uuid4()
    token = await store.stage(task_id, "summary", "desc")
    assert (await store.claim(task_id, 3)).decision == "publishing"
    assert (await store.get(task_id))["attempts"] == 0
    assert await store.rejected(task_id, "uncertain", publication_token=token)
    assert (await store.claim(task_id, 3)).decision == "terminal"


async def test_expiry_cancels_only_abandoned_publications(store):
    old, fresh = uuid4(), uuid4()
    await store.stage(old, "summary", "desc")
    await age(store, old)
    await store.stage(fresh, "summary", "desc")
    pending = await accepted(store)
    await age(store, pending)
    assert await store.expire_publications(30) == 1
    assert (await store.get(old))["status"] == TaskState.REJECTED
    assert (await store.get(fresh))["status"] == TaskState.PUBLISHING
    assert (await store.get(pending))["status"] == TaskState.PENDING


async def test_old_publisher_cannot_commit_or_cancel_new_admission(store):
    task_id = uuid4()
    old = await store.stage(task_id, "summary", "desc")
    await age(store, task_id)
    await store.expire_publications(30)
    new = await store.stage(task_id, "summary", "desc")
    assert not await store.published(task_id, publication_token=old)
    assert not await store.rejected(task_id, "late error", publication_token=old)
    assert await store.published(task_id, publication_token=new)


async def test_concurrent_admission_owner_is_not_overwritten(store):
    task_id = uuid4()
    await store.stage(task_id, "summary", "desc")
    with pytest.raises(PublicationInProgress):
        await store.stage(task_id, "summary", "desc")
    with pytest.raises(ValueError, match="different payload"):
        await store.stage(task_id, "changed", "desc")


async def test_message_payload_must_match_durable_task(store):
    task_id = await accepted(store)
    claim = await store.claim(task_id, 3, expected_payload=("tampered", "desc"))
    assert claim.decision == "invalid"
    assert (await store.get(task_id))["attempts"] == 0

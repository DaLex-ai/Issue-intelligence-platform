"""Shared transactional admission for API and streaming batch source."""

from uuid import UUID

from issue_intelligence_platform.pipeline.rabbit import (
    ConfirmingPublisher,
    PublishUncertain,
    QueueFull,
)
from issue_intelligence_platform.pipeline.store import TaskStore


async def submit(
    store: TaskStore,
    publisher: ConfirmingPublisher,
    task_id: UUID,
    summary: str,
    description: str,
) -> bool:
    """Return False if previously accepted; raise on overload / ambiguity.

    PUBLISHING is deliberately not PENDING. If the broker rejects, mark it
    REJECTED before responding 503. Worker consults the same row on consume.
    """
    new_attempt = await store.stage(task_id, summary, description)
    if not new_attempt:
        return False
    try:
        await publisher.publish(task_id, summary, description)
    except (QueueFull, PublishUncertain) as exc:
        cancelled = await store.rejected(task_id, type(exc).__name__)
        if not cancelled:
            # Another consumer has already committed a terminal result. Do not
            # report 503 for a completed task after a lost publisher confirm.
            return True
        raise
    await store.published(task_id)
    return True

"""Shared transactional admission for API and streaming batch source."""

from uuid import UUID

from issue_intelligence_platform.pipeline.rabbit import (
    ConfirmingPublisher,
    PublishUncertain,
    QueueFull,
)
from issue_intelligence_platform.pipeline.store import PublicationInProgress, TaskStore


async def submit(
    store: TaskStore,
    publisher: ConfirmingPublisher,
    task_id: UUID,
    summary: str,
    description: str,
) -> bool:
    """Return False if previously accepted; raise on overload / ambiguity.

    Consumers defer PUBLISHING tasks until confirms and SQL admission commit.
    An uncertain publish is cancelled, not automatically replayed. The timestamp
    token prevents a late publisher from committing a newer batch admission.
    """
    try:
        new_attempt = await store.stage(task_id, summary, description)
    except PublicationInProgress as exc:
        raise PublishUncertain("another admission is in progress") from exc
    if not new_attempt:
        return False
    try:
        await publisher.publish(task_id, summary, description)
    except (QueueFull, PublishUncertain) as exc:
        await store.rejected(task_id, type(exc).__name__, publication_token=new_attempt)
        raise
    if not await store.published(task_id, publication_token=new_attempt):
        raise PublishUncertain("admission expired before confirmation was committed")
    return True

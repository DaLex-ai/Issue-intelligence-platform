from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from issue_intelligence_platform.pipeline.admission import submit
from issue_intelligence_platform.pipeline.rabbit import PublishUncertain, QueueFull


@pytest.mark.parametrize("error", [QueueFull(), PublishUncertain()])
async def test_publish_refusal_has_no_pending_state(error):
    store = AsyncMock()
    store.stage.return_value = "token"
    store.rejected.return_value = True
    pub = AsyncMock()
    pub.publish.side_effect = error
    task_id = uuid4()
    with pytest.raises(type(error)):
        await submit(store, pub, task_id, "summary", "description")
    store.rejected.assert_awaited_once()
    store.published.assert_not_awaited()


async def test_confirm_after_stage_moves_to_pending():
    store = AsyncMock()
    store.stage.return_value = "token"
    pub = AsyncMock()
    task_id = uuid4()
    assert await submit(store, pub, task_id, "summary", "description")
    store.published.assert_awaited_once_with(task_id, publication_token="token")


async def test_uncertain_publication_is_not_reported_as_accepted():
    store = AsyncMock()
    store.stage.return_value = "token"
    store.rejected.return_value = False
    pub = AsyncMock()
    pub.publish.side_effect = PublishUncertain()
    with pytest.raises(PublishUncertain):
        await submit(store, pub, uuid4(), "summary", "description")
    store.published.assert_not_awaited()


async def test_batch_resumes_without_republishing_completed_message():
    store = AsyncMock()
    store.stage.return_value = False
    pub = AsyncMock()
    assert not await submit(store, pub, uuid4(), "summary", "description")
    pub.publish.assert_not_awaited()


async def test_expired_admission_cannot_return_202():
    store = AsyncMock()
    store.stage.return_value = "token"
    store.published.return_value = False
    with pytest.raises(PublishUncertain):
        await submit(store, AsyncMock(), uuid4(), "summary", "description")


async def test_competing_publisher_does_not_cancel_owner():
    from issue_intelligence_platform.pipeline.store import PublicationInProgress

    store = AsyncMock()
    store.stage.side_effect = PublicationInProgress()
    pub = AsyncMock()
    with pytest.raises(PublishUncertain):
        await submit(store, pub, uuid4(), "summary", "description")
    pub.publish.assert_not_awaited()
    store.rejected.assert_not_awaited()

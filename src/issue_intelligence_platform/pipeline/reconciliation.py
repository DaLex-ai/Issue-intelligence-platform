"""Cancel abandoned publications; retries never infer before admission commits."""

import asyncio
import logging

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.pipeline.store import TaskStore

logger = logging.getLogger(__name__)


async def reconcile_loop(store: TaskStore) -> None:
    while True:
        try:
            count = await store.expire_publications(
                settings.publication_timeout_seconds
            )
            if count:
                logger.warning("expired_publications count=%s", count)
        except Exception:
            logger.exception("publication_reconciliation_failed")
        await asyncio.sleep(settings.publication_reconcile_seconds)

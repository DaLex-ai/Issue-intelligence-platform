"""Streaming JSONL input: one task in memory and stable task IDs for restart.

Usage: python -m issue_intelligence_platform.pipeline.batch data/issues.jsonl
"""

import argparse
import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from issue_intelligence_platform.core import telemetry as obs
from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.core.logging import configure_logging
from issue_intelligence_platform.db.database import (
    engine,
    initialize_schema,
    session_factory,
)
from issue_intelligence_platform.pipeline.admission import submit
from issue_intelligence_platform.pipeline.rabbit import (
    ConfirmingPublisher,
    PublishUncertain,
    QueueFull,
)
from issue_intelligence_platform.pipeline.schemas import TaskCreate
from issue_intelligence_platform.pipeline.store import TaskStore

logger = logging.getLogger(__name__)


async def run(path: Path) -> None:
    publisher = ConfirmingPublisher()
    store = TaskStore(session_factory)
    backoff = settings.batch_backoff_min_seconds
    confirmed = 0
    rejected = 0
    began = time.monotonic()
    try:
        await initialize_schema()
        await publisher.start()
        # Stream source, O(max_record_bytes) RAM; never read entire file or
        # enqueue into an unbounded asyncio.Queue.
        with path.open(encoding="utf-8") as source:
            for index, line in enumerate(source):
                if not line.strip():
                    continue
                try:
                    task = TaskCreate.model_validate(json.loads(line))
                except (ValueError, ValidationError) as exc:
                    raise ValueError(
                        f"Invalid JSONL input at line {index + 1}"
                    ) from exc
                # Stable across restarts of this file at its source path.
                digest = hashlib.sha256(line.encode("utf-8")).hexdigest()
                task_id = uuid5(NAMESPACE_URL, f"{path.resolve()}:{index}:{digest}")
                with obs.task_scope(task_id), obs.span("batch task") as current:
                    current.set_attribute("task.id", str(task_id))
                    while True:
                        try:
                            await store.expire_publications(
                                settings.publication_timeout_seconds
                            )
                            accepted = await submit(
                                store,
                                publisher,
                                task_id,
                                task.summary,
                                task.description,
                            )
                            if accepted:
                                confirmed += 1
                                obs.count("iip_source_published")
                                logger.info(
                                    "publish_confirm",
                                    extra={
                                        "event": "publish_confirm",
                                        "count": confirmed,
                                    },
                                )
                            backoff = settings.batch_backoff_min_seconds
                            break
                        except (QueueFull, PublishUncertain) as exc:
                            rejected += 1
                            if isinstance(exc, QueueFull):
                                obs.count("iip_source_broker_rejections")
                            with obs.span("batch backoff") as pause:
                                pause.set_attribute("retry.delay_seconds", backoff)
                                obs.fail(exc)
                                logger.warning(
                                    "batch_backoff",
                                    extra={
                                        "event": "publish_rejected",
                                        "count": rejected,
                                        "delay_seconds": backoff,
                                        "error_type": type(exc).__name__,
                                    },
                                )
                                started = time.monotonic()
                                await asyncio.sleep(backoff)
                                obs.wait_seconds(time.monotonic() - started)
                            backoff = min(
                                2 * backoff, settings.batch_backoff_max_seconds
                            )
                if index % 25 == 0:
                    seconds = max(time.monotonic() - began, 1e-9)
                    logger.info(
                        "batch_progress line=%s accepted=%s rejected=%s confirmed_rate=%.2f/s",
                        index + 1,
                        confirmed,
                        rejected,
                        confirmed / seconds,
                    )
    finally:
        logger.info("graceful_shutdown_started")
        try:
            await publisher.close()
        finally:
            await engine.dispose()
        logger.info("graceful_shutdown_completed")
    seconds = max(time.monotonic() - began, 1e-9)
    logger.info(
        "batch_done accepted=%s rejected=%s rate=%.3f/s",
        confirmed,
        rejected,
        confirmed / seconds,
    )


def main() -> None:
    obs.setup("batch")
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.file))
    except Exception:
        logger.exception("batch_exited")
        raise SystemExit(1) from None
    finally:
        obs.flush()
        obs.shutdown()


if __name__ == "__main__":
    main()

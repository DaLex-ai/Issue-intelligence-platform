"""Streaming JSONL input: one task in memory and stable task IDs for restart.

Usage: python -m issue_intelligence_platform.pipeline.batch data/issues.jsonl
"""

import argparse
import asyncio
import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from issue_intelligence_platform.core.config import settings
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
    await initialize_schema()
    publisher = ConfirmingPublisher()
    store = TaskStore(session_factory)
    backoff = settings.batch_backoff_min_seconds
    confirmed = 0
    rejected = 0
    began = time.monotonic()
    try:
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
                while True:
                    try:
                        accepted = await submit(
                            store,
                            publisher,
                            task_id,
                            task.summary,
                            task.description,
                        )
                        if accepted:
                            confirmed += 1
                            print(
                                "METRIC "
                                + json.dumps(
                                    {
                                        "event": "publish_confirm",
                                        "timestamp": datetime.now(UTC).isoformat(),
                                        "count": confirmed,
                                    }
                                ),
                                flush=True,
                            )
                        backoff = settings.batch_backoff_min_seconds
                        break
                    except (QueueFull, PublishUncertain):
                        rejected += 1
                        print(
                            "METRIC "
                            + json.dumps(
                                {
                                    "event": "publish_rejected",
                                    "timestamp": datetime.now(UTC).isoformat(),
                                    "count": rejected,
                                }
                            ),
                            flush=True,
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(2 * backoff, settings.batch_backoff_max_seconds)
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
        await publisher.close()
        await engine.dispose()
    seconds = max(time.monotonic() - began, 1e-9)
    logger.info(
        "batch_done accepted=%s rejected=%s rate=%.3f/s",
        confirmed,
        rejected,
        confirmed / seconds,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    asyncio.run(run(args.file))


if __name__ == "__main__":
    main()

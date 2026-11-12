"""Defense-only fault injection, enabled by the isolated Compose override."""

import asyncio

from issue_intelligence_platform.core import telemetry
from issue_intelligence_platform.core.logging import configure_logging
from issue_intelligence_platform.pipeline import worker

predict = worker.infer


def infer(summary, description):
    if summary == "iip-demo-poison":
        raise ValueError("controlled inference failure")
    return predict(summary, description)


if __name__ == "__main__":
    worker.infer = infer
    telemetry.setup("worker")
    configure_logging()
    try:
        asyncio.run(worker.run())
    finally:
        telemetry.shutdown()

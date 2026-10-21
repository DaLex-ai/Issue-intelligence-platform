"""Start with: python -m issue_intelligence_platform.pipeline.worker.

Each process owns one loaded MLflow model and an executor of C threads. FastStream
limits AMQP unacked deliveries to the same number of active inference slots.
"""

import asyncio
import json
import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import perf_counter
from typing import Any

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.schemas import Channel
from pydantic import ValidationError

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.db.database import (
    engine,
    initialize_schema,
    session_factory,
)
from issue_intelligence_platform.ml.model_service import model_service
from issue_intelligence_platform.pipeline.rabbit import ConfirmingPublisher, work_queue
from issue_intelligence_platform.pipeline.schemas import TaskPayload
from issue_intelligence_platform.pipeline.store import TaskStore

logger = logging.getLogger(__name__)
store = TaskStore(session_factory)
executor: ThreadPoolExecutor | None = None


def infer(summary: str, description: str) -> str:
    if settings.inference_delay_seconds > 0:
        # Deliberate deterministic latency INSIDE executor, never in event loop.
        time.sleep(settings.inference_delay_seconds)
    return model_service.predict(summary=summary, description=description)


def timed_infer(summary: str, description: str) -> tuple[str, float]:
    """Measure inference without thread pool waiting time."""
    started = perf_counter()
    prediction = infer(summary, description)
    return prediction, perf_counter() - started


async def handle_task(body: dict[str, Any], msg: RabbitMessage) -> None:
    """One AMQP delivery: commit before ACK; permanent failures reject to DLX."""
    try:
        # Raw-decoded AMQP bytes may contain invalid JSON, not only invalid
        # task fields. Both cases must reach DLQ rather than poison the decoder.
        if isinstance(body, (bytes, bytearray, str)):
            body = json.loads(body)
        payload = TaskPayload.model_validate(body)
    except (ValidationError, TypeError, ValueError, UnicodeDecodeError):
        logger.warning("malformed_delivery_to_dlq")
        await msg.reject(requeue=False)
        return

    if msg.raw_message.redelivered:
        # Only a broker-marked redelivery can recover a PROCESSING claim. A
        # separately published duplicate never steals the live delivery.
        await store.recover(payload.task_id)

    try:
        decision = await store.claim(payload.task_id, settings.worker_max_attempts)
        if decision in {"terminal", "busy"}:
            await msg.ack()
            return
        if decision in {"unknown", "exhausted"}:
            await msg.reject(requeue=False)
            return

        if executor is None:
            raise RuntimeError("worker model/executor not initialized")
        loop = asyncio.get_running_loop()
        prediction, inference_seconds = await loop.run_in_executor(
            executor, partial(timed_infer, payload.summary, payload.description)
        )
        await store.succeed(
            payload.task_id,
            prediction,
            settings.mlflow_model_name,
            settings.mlflow_model_alias,
            model_service.model_version or "unknown",
            inference_seconds,
        )
        # If an API request was cancelled after an uncertain publish, succeed()
        # skips the REJECTED record, so 503 never creates a result.
        await msg.ack()
    except Exception as exc:
        logger.exception(
            "task_processing_failed", extra={"task_id": str(payload.task_id)}
        )
        try:
            exhausted = await store.fail(
                payload.task_id, str(exc), settings.worker_max_attempts
            )
        except Exception:
            # DB unavailable: never ACK; avoid a CPU-intensive requeue loop.
            await asyncio.sleep(settings.worker_retry_delay_seconds)
            await msg.nack(requeue=True)
            return
        if exhausted:
            await msg.reject(requeue=False)
        else:
            await asyncio.sleep(settings.worker_retry_delay_seconds)
            await msg.nack(requeue=True)


broker = RabbitBroker(
    settings.rabbit_url,
    graceful_timeout=settings.worker_graceful_timeout,
    default_channel=Channel(prefetch_count=settings.worker_prefetch_count),
)


async def raw_decoder(msg: RabbitMessage) -> bytes:
    """Deliver untrusted bytes to handler even if the JSON is corrupt."""
    return msg.body


@broker.subscriber(
    work_queue(),
    ack_policy=AckPolicy.MANUAL,
    decoder=raw_decoder,
)
async def on_message(body: bytes, msg: RabbitMessage) -> None:
    await handle_task(body, msg)


async def health_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await asyncio.wait_for(reader.read(2048), timeout=2)
        response = b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{"status":"ok"}'
        writer.write(response)
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def run() -> None:
    global executor
    if (
        settings.enable_backpressure
        and settings.worker_prefetch_count != settings.worker_concurrency
    ):
        logger.warning(
            "prefetch_count=%d differs from executor slots=%d; "
            "smaller values underutilize slots, larger values buffer unacked work",
            settings.worker_prefetch_count,
            settings.worker_concurrency,
        )
    if settings.worker_concurrency < 1 or settings.worker_max_attempts < 1:
        raise ValueError("worker concurrency and attempts must be positive")
    await initialize_schema()
    topology = ConfirmingPublisher()
    await topology.start()
    await topology.close()
    # Load MLflow alias exactly once before opening AMQP consumers.
    await asyncio.to_thread(model_service.load)
    executor = ThreadPoolExecutor(max_workers=settings.worker_concurrency)
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)
    server = await asyncio.start_server(
        health_handler, "0.0.0.0", settings.worker_health_port
    )
    try:
        await broker.start()
        logger.info("worker_started")
        await shutdown.wait()
    finally:
        # FastStream's graceful_timeout waits for in-flight subscribers.
        # Docker stop_grace_period must exceed this duration.
        await broker.stop()
        server.close()
        await server.wait_closed()
        executor.shutdown(wait=True)
        executor = None
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

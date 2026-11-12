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
from contextlib import suppress
from contextvars import copy_context
from datetime import UTC, datetime
from functools import partial
from time import perf_counter
from typing import Any
from uuid import UUID

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.schemas import Channel
from opentelemetry.trace import SpanKind
from pydantic import ValidationError

from issue_intelligence_platform.core import telemetry as obs
from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.core.logging import configure_logging
from issue_intelligence_platform.db.database import (
    engine,
    initialize_schema,
    session_factory,
)
from issue_intelligence_platform.ml.model_service import model_service
from issue_intelligence_platform.pipeline.rabbit import ConfirmingPublisher, work_queue
from issue_intelligence_platform.pipeline.reconciliation import reconcile_loop
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


async def renew_claim(task_id, attempt: int) -> None:
    while True:
        await asyncio.sleep(settings.worker_heartbeat_seconds)
        try:
            if not await store.renew(task_id, attempt):
                return
        except Exception:
            # A failed renewal cannot confer ownership. Completion still checks
            # the fencing token; a recovered worker may acquire the expired lease.
            logger.exception("claim_renewal_failed")


async def handle_task(body: dict[str, Any], msg: RabbitMessage) -> None:
    raw = msg.raw_message
    headers = getattr(raw, "headers", None)
    headers = headers if isinstance(headers, dict) else {}
    correlation = getattr(raw, "correlation_id", None)
    try:
        correlation = str(UUID(correlation)) if isinstance(correlation, str) else None
    except ValueError:
        correlation = None
    with (
        obs.task_scope(correlation),
        obs.span(
            "rabbitmq process",
            context=obs.parent_context(headers),
            kind=SpanKind.CONSUMER,
        ) as current,
    ):
        current.set_attribute("messaging.system", "rabbitmq")
        current.set_attribute("messaging.destination.name", settings.rabbit_queue)
        if raw.redelivered:
            obs.count("iip_worker_redeliveries")
            current.add_event("redelivery")
        try:
            enqueued = int(headers.get("iip-enqueued-ns", "0"))
        except (TypeError, ValueError):
            enqueued = 0
        if enqueued and not raw.redelivered:
            current.set_attribute(
                "messaging.queue_wait_seconds",
                max(0.0, (time.time_ns() - enqueued) / 1e9),
            )
        obs.active(1)
        try:
            await _handle_task(body, msg, correlation, current)
        except Exception as exc:
            obs.fail(exc)
            logger.exception("delivery_failed")
            raise
        finally:
            obs.active(-1)


def traced_infer(summary: str, description: str) -> tuple[str, float]:
    with obs.span("inference") as current:
        current.set_attribute("model.alias", settings.mlflow_model_alias)
        current.set_attribute("model.version", model_service.model_version or "unknown")
        started = perf_counter()
        try:
            return timed_infer(summary, description)
        except Exception as exc:
            obs.fail(exc)
            logger.exception("inference_failed")
            raise
        finally:
            obs.observe("iip_inference_seconds", perf_counter() - started)


async def dead_letter(msg, reason, attempt=0):
    await msg.reject(requeue=False)
    obs.count("iip_worker_tasks", result="dlq")
    obs.fail(reason)
    from opentelemetry import trace

    trace.get_current_span().add_event(
        "dead_letter", {"reason": reason, "attempt": attempt}
    )
    logger.warning("task_sent_to_dlq", extra={"attempt": attempt, "error_type": reason})


async def _handle_task(body, msg, correlation, current):
    """One AMQP delivery: commit before ACK; permanent failures reject to DLX."""
    try:
        # Raw-decoded AMQP bytes may contain invalid JSON, not only invalid
        # task fields. Both cases must reach DLQ rather than poison the decoder.
        if isinstance(body, (bytes, bytearray, str)):
            body = json.loads(body)
        payload = TaskPayload.model_validate(body)
        if correlation and correlation != str(payload.task_id):
            raise ValueError("correlation ID does not match task ID")
    except (ValidationError, TypeError, ValueError, UnicodeDecodeError):
        await dead_letter(msg, "malformed_delivery")
        return

    current.set_attribute("task.id", str(payload.task_id))
    with obs.task_scope(payload.task_id):
        await process_claim(payload, msg, current)


async def process_claim(payload, msg, current):
    claim = None
    heartbeat = None
    try:
        claim = await store.claim(
            payload.task_id,
            settings.worker_max_attempts,
            redelivered=msg.raw_message.redelivered,
            expected_payload=(payload.summary, payload.description),
            lease_seconds=settings.worker_claim_timeout_seconds,
        )
        current.set_attribute("task.attempt", claim.attempt)
        current.set_attribute("task.claim_decision", claim.decision)
        if claim.decision == "terminal":
            await msg.ack()
            return
        if claim.decision in {"unknown", "invalid", "failed"}:
            await dead_letter(msg, claim.decision, claim.attempt)
            return
        if claim.decision in {"busy", "publishing"}:
            await asyncio.sleep(settings.worker_retry_delay_seconds)
            await msg.nack(requeue=True)
            return
        if executor is None:
            raise RuntimeError("worker model/executor not initialized")
        heartbeat = asyncio.create_task(renew_claim(payload.task_id, claim.attempt))
        loop = asyncio.get_running_loop()
        logger.info("task_processing_started", extra={"attempt": claim.attempt})
        prediction, inference_seconds = await loop.run_in_executor(
            executor,
            copy_context().run,
            partial(traced_infer, claim.summary, claim.description),
        )
        with obs.span("postgresql write result", kind=SpanKind.CLIENT) as db_span:
            db_span.set_attribute("db.system.name", "postgresql")
            try:
                committed = await store.succeed(
                    payload.task_id,
                    prediction,
                    settings.mlflow_model_name,
                    settings.mlflow_model_alias,
                    model_service.model_version or "unknown",
                    inference_seconds,
                    attempt=claim.attempt,
                )
            except Exception as exc:
                obs.fail(exc)
                raise
        if committed:
            await msg.ack()
            obs.count("iip_worker_tasks", result="ok")
            if claim.created_at is not None:
                duration = (
                    datetime.now(UTC) - claim.created_at.replace(tzinfo=UTC)
                ).total_seconds()
                obs.observe("iip_task_duration_seconds", max(0, duration))
            logger.info("task_completed", extra={"attempt": claim.attempt})
        else:
            # The new owner may still be processing, or have failed. Re-read
            # the durable state on redelivery rather than silently consuming it.
            await asyncio.sleep(settings.worker_retry_delay_seconds)
            await msg.nack(requeue=True)
    except Exception as exc:
        obs.fail(exc)
        obs.count("iip_worker_tasks", result="error")
        logger.exception(
            "task_processing_failed", extra={"attempt": claim.attempt if claim else 0}
        )
        exhausted = False
        if claim is not None and claim.decision == "claimed":
            try:
                exhausted = await store.fail(
                    payload.task_id,
                    str(exc),
                    settings.worker_max_attempts,
                    attempt=claim.attempt,
                )
            except Exception:
                logger.exception("task_failure_state_unavailable")
        if exhausted:
            await dead_letter(msg, "attempts_exhausted", claim.attempt)
        else:
            await asyncio.sleep(settings.worker_retry_delay_seconds)
            await msg.nack(requeue=True)
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat


broker = RabbitBroker(
    settings.rabbit_url,
    graceful_timeout=settings.worker_graceful_timeout,
    logger=logging.getLogger("faststream.iip"),
    log_level=logging.DEBUG,
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
        from sqlalchemy import text

        async def dependencies_ready():
            if executor is None or model_service.model is None:
                raise RuntimeError("model is not ready")
            if not await broker.ping(timeout=1):
                raise RuntimeError("broker is unavailable")
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

        try:
            await asyncio.wait_for(dependencies_ready(), timeout=2)
            status, body = b"200 OK", b'{"status":"ok"}'
        except Exception:
            status, body = b"503 Service Unavailable", b'{"status":"unavailable"}'
        response = (
            b"HTTP/1.1 "
            + status
            + b"\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
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
    server = None
    reconciler = None
    registered_signals = []
    fallback_signals = {}
    topology = ConfirmingPublisher()
    try:
        await initialize_schema()
        try:
            await topology.start()
        finally:
            await topology.close()
        await asyncio.to_thread(model_service.load)
        executor = ThreadPoolExecutor(max_workers=settings.worker_concurrency)
        shutdown = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown.set)
                registered_signals.append(sig)
            except NotImplementedError:
                # Windows has no asyncio signal handlers. A native handler
                # still schedules shutdown safely in this event loop.
                fallback_signals[sig] = signal.getsignal(sig)
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(shutdown.set))
        await broker.start()
        reconciler = asyncio.create_task(reconcile_loop(store))
        server = await asyncio.start_server(
            health_handler, "0.0.0.0", settings.worker_health_port
        )
        logger.info("worker_started")
        await shutdown.wait()
    finally:
        logger.info("graceful_shutdown_started")
        if reconciler is not None:
            reconciler.cancel()
            with suppress(asyncio.CancelledError):
                await reconciler
        try:
            await broker.stop()
        finally:
            if server is not None:
                server.close()
                await server.wait_closed()
            if executor is not None:
                # Docker's stop grace bounds a non-cooperative native model.
                await asyncio.to_thread(executor.shutdown, wait=True)
                executor = None
            for sig in registered_signals:
                asyncio.get_running_loop().remove_signal_handler(sig)
            for sig, handler in fallback_signals.items():
                signal.signal(sig, handler)
            await engine.dispose()
            logger.info("graceful_shutdown_completed")


if __name__ == "__main__":
    obs.setup("worker")
    configure_logging()
    try:
        asyncio.run(run())
    except Exception:
        logger.exception("worker_exited")
        raise SystemExit(1) from None
    finally:
        obs.shutdown()

"""Real RabbitMQ integration tests with deterministic socket cleanup.

The broker is probed with *synchronous* Pika connections before the async
pytest event loop starts. Failed aio-pika startup probes on Windows can leave
Proactor transports pending; under ``-W error`` these may later surface as
PytestUnraisableExceptionWarning in unrelated tests.
"""

import asyncio
import time
from uuid import uuid4

import aio_pika
import pika
import pytest
from testcontainers.core.container import DockerContainer

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.pipeline.rabbit import ConfirmingPublisher, QueueFull


@pytest.fixture(scope="module")
def rabbit_broker_url():
    """Start a real broker, confirm AMQP readiness, then yield its URL."""
    with DockerContainer("rabbitmq:4.1-management-alpine").with_exposed_ports(
        5672
    ) as broker:
        host = broker.get_container_host_ip()
        # Windows often resolves 'localhost' to ::1 first. Use IPv4 for the
        # published Docker Desktop port to avoid unnecessary connection churn.
        if host in ("localhost", "::1"):
            host = "127.0.0.1"
        port = broker.get_exposed_port(5672)
        url = f"amqp://guest:guest@{host}:{port}/"
        wait_for_broker(url)
        yield url


def wait_for_broker(url: str, timeout: float = 90.0) -> None:
    """Verify a complete AMQP handshake, never create a failed asyncio transport."""
    params = pika.URLParameters(url)
    params.connection_attempts = 1
    params.socket_timeout = 2
    params.stack_timeout = 4
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        connection = None
        try:
            connection = pika.BlockingConnection(params)
            return
        except (pika.exceptions.AMQPError, OSError, TimeoutError) as exc:
            last_error = exc
            time.sleep(0.5)
        finally:
            if connection is not None and connection.is_open:
                connection.close()
    raise TimeoutError(
        "RabbitMQ Testcontainer did not become AMQP-ready"
    ) from last_error


async def close_publisher(pub: ConfirmingPublisher) -> None:
    """Close both channel and connection before the pytest loop is disposed."""
    try:
        if pub.channel is not None and not pub.channel.is_closed:
            await pub.channel.close()
    finally:
        await pub.close()
        # Let asyncio/Windows Proactor process transport-close callbacks.
        await asyncio.sleep(0.2)


@pytest.mark.integration
async def test_real_broker_reject_publish_then_recover(monkeypatch, rabbit_broker_url):
    monkeypatch.setattr(settings, "rabbit_url", rabbit_broker_url)
    monkeypatch.setattr(settings, "rabbit_queue", "lab4." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlq", "lab4.dlq." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlx", "lab4.dead." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_max_length", 1)
    monkeypatch.setattr(settings, "enable_backpressure", True)

    pub = ConfirmingPublisher()
    try:
        await pub.start()
        await pub.publish(uuid4(), "first", "")
        with pytest.raises(QueueFull):
            await pub.publish(uuid4(), "second", "")

        connection = await aio_pika.connect_robust(rabbit_broker_url)
        try:
            channel = await connection.channel()
            try:
                queue = await channel.declare_queue(settings.rabbit_queue, passive=True)
                msg = await queue.get(timeout=5)
                assert msg is not None
                await msg.ack()
            finally:
                await channel.close()
        finally:
            await connection.close()

        await pub.publish(uuid4(), "third", "")
    finally:
        await close_publisher(pub)


@pytest.mark.integration
async def test_real_dead_letter_exchange_after_reject(monkeypatch, rabbit_broker_url):
    monkeypatch.setattr(settings, "rabbit_url", rabbit_broker_url)
    monkeypatch.setattr(settings, "rabbit_queue", "lab4." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlq", "lab4.dlq." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlx", "lab4.dead." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_max_length", 5)
    monkeypatch.setattr(settings, "enable_backpressure", True)

    pub = ConfirmingPublisher()
    try:
        await pub.start()
        await pub.publish(uuid4(), "invalid", "")

        connection = await aio_pika.connect_robust(rabbit_broker_url)
        try:
            channel = await connection.channel()
            try:
                ready = await channel.declare_queue(settings.rabbit_queue, passive=True)
                msg = await ready.get(timeout=5)
                assert msg is not None
                await msg.reject(requeue=False)
                dlq = await channel.declare_queue(settings.rabbit_dlq, passive=True)
                failed = await dlq.get(timeout=5)
                assert failed is not None
                assert b"invalid" in failed.body
                await failed.ack()
            finally:
                await channel.close()
        finally:
            await connection.close()
    finally:
        await close_publisher(pub)


@pytest.fixture(scope="module")
def postgres_url():
    with (
        DockerContainer("postgres:17-alpine")
        .with_env("POSTGRES_USER", "reliability")
        .with_env("POSTGRES_PASSWORD", "isolated-test-password")
        .with_env("POSTGRES_DB", "reliability")
        .with_exposed_ports(5432)
    ) as postgres:
        host = postgres.get_container_host_ip()
        if host in ("localhost", "::1"):
            host = "127.0.0.1"
        port = postgres.get_exposed_port(5432)
        yield f"postgresql+asyncpg://reliability:isolated-test-password@{host}:{port}/reliability"


@pytest.fixture
async def pg_store(postgres_url):
    from asyncpg.exceptions import PostgresError
    from sqlalchemy import delete, text
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from issue_intelligence_platform.pipeline.store import Base, Task, TaskStore

    engine = create_async_engine(postgres_url)
    try:
        deadline = time.monotonic() + 90
        while True:
            try:
                async with engine.begin() as connection:
                    await connection.execute(text("SELECT 1"))
                    await connection.run_sync(Base.metadata.create_all)
                    # Only this module's disposable database; each case starts clean.
                    await connection.execute(delete(Task))
                break
            except (SQLAlchemyError, OSError, PostgresError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.5)
        yield TaskStore(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()


@pytest.fixture
async def isolated_publisher(monkeypatch, rabbit_broker_url):
    monkeypatch.setattr(settings, "rabbit_url", rabbit_broker_url)
    monkeypatch.setattr(settings, "rabbit_queue", "reliability." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlq", "reliability.dlq." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_dlx", "reliability.dead." + uuid4().hex)
    monkeypatch.setattr(settings, "rabbit_max_length", 20)
    monkeypatch.setattr(settings, "enable_backpressure", True)
    monkeypatch.setattr(settings, "worker_retry_delay_seconds", 0.01)
    pub = ConfirmingPublisher()
    try:
        await pub.start()
        yield pub
    finally:
        await close_publisher(pub)


async def get_delivery(queue):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        msg = await queue.get(fail=False, timeout=3)
        if msg is not None:
            return msg
        await asyncio.sleep(0.1)
    raise TimeoutError("expected delivery was not received")


def worker_delivery(msg):
    from types import SimpleNamespace

    return SimpleNamespace(
        raw_message=msg, ack=msg.ack, nack=msg.nack, reject=msg.reject
    )


async def accept_task(store, publisher, task_id):
    token = await store.stage(task_id, "Fails", "")
    await publisher.publish(task_id, "Fails", "")
    assert await store.published(task_id, publication_token=token)


@pytest.mark.integration
async def test_postgres_concurrent_claim_and_stale_owner(pg_store):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from issue_intelligence_platform.pipeline.store import Task

    task_id = uuid4()
    token = await pg_store.stage(task_id, "Fails", "")
    await pg_store.published(task_id, publication_token=token)
    claims = await asyncio.gather(
        pg_store.claim(task_id, 3), pg_store.claim(task_id, 3)
    )
    assert sorted(c.decision for c in claims) == ["busy", "claimed"]
    old = next(c for c in claims if c.decision == "claimed")
    async with pg_store.sessions.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == str(task_id))
            .values(updated_at=datetime.now(UTC) - timedelta(seconds=60))
        )
    new = await pg_store.claim(task_id, 3, redelivered=True)
    assert new.attempt == old.attempt + 1
    assert not await pg_store.succeed(
        task_id, "stale", "m", "champion", "1", 0.1, attempt=old.attempt
    )
    assert not await pg_store.fail(task_id, "stale", 3, attempt=old.attempt)
    assert await pg_store.succeed(
        task_id, "current", "m", "champion", "2", 0.1, attempt=new.attempt
    )


@pytest.mark.integration
async def test_postgres_concurrent_stage_does_not_overwrite_admission(pg_store):
    from datetime import datetime

    from issue_intelligence_platform.pipeline.store import PublicationInProgress

    task_id = uuid4()
    results = await asyncio.gather(
        pg_store.stage(task_id, "Fails", ""),
        pg_store.stage(task_id, "Fails", ""),
        return_exceptions=True,
    )
    assert sum(isinstance(r, datetime) for r in results) == 1
    assert sum(isinstance(r, PublicationInProgress) for r in results) == 1


@pytest.mark.integration
async def test_failed_commit_then_channel_loss_reaches_real_dlq(
    pg_store, isolated_publisher, monkeypatch
):
    from issue_intelligence_platform.pipeline import worker

    pub = isolated_publisher
    task_id = uuid4()
    await accept_task(pg_store, pub, task_id)
    claim = await pg_store.claim(task_id, 1)
    # Consumer receives the message; crash after FAILED commits, before reject.
    channel = await pub.connection.channel()
    queue = await channel.declare_queue(settings.rabbit_queue, passive=True)
    await get_delivery(queue)
    assert await pg_store.fail(task_id, "permanent", 1, attempt=claim.attempt)
    await channel.close()
    queue = await pub.channel.declare_queue(settings.rabbit_queue, passive=True)
    redelivery = await get_delivery(queue)
    assert redelivery.redelivered
    monkeypatch.setattr(worker, "store", pg_store)
    await worker.handle_task(redelivery.body, worker_delivery(redelivery))
    dlq = await pub.channel.declare_queue(settings.rabbit_dlq, passive=True)
    failed = await get_delivery(dlq)
    assert str(task_id).encode() in failed.body
    await failed.ack()
    assert (await pg_store.get(task_id))["status"] == "FAILED"


@pytest.mark.integration
async def test_real_pipeline_defers_until_admission_then_persists_result(
    pg_store, isolated_publisher, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor

    from issue_intelligence_platform.pipeline import worker

    pub = isolated_publisher
    task_id = uuid4()
    token = await pg_store.stage(task_id, "Fails", "")
    await pub.publish(task_id, "Fails", "")
    queue = await pub.channel.declare_queue(settings.rabbit_queue, passive=True)
    msg = await get_delivery(queue)
    monkeypatch.setattr(worker, "store", pg_store)
    await worker.handle_task(msg.body, worker_delivery(msg))
    assert (await pg_store.get(task_id))["attempts"] == 0
    assert await pg_store.published(task_id, publication_token=token)
    msg = await get_delivery(queue)
    monkeypatch.setattr(worker, "infer", lambda *a: "High")
    monkeypatch.setattr(worker.model_service, "model_version", "integration-test")
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        await worker.handle_task(msg.body, worker_delivery(msg))
    result = await pg_store.get(task_id)
    assert result["status"] == "SUCCEEDED"
    assert result["prediction"] == "High"
    assert result["model_version"] == "integration-test"
    assert await queue.get(fail=False) is None


@pytest.mark.integration
async def test_real_overflow_cancels_sql_admission(
    pg_store, isolated_publisher, monkeypatch
):
    from issue_intelligence_platform.pipeline.admission import submit

    pub = isolated_publisher
    # Existing isolated queue has capacity 20 and no consumers.
    for _ in range(20):
        await pub.publish(uuid4(), "filler", "")
    task_id = uuid4()
    with pytest.raises(QueueFull):
        await submit(pg_store, pub, task_id, "Fails", "")
    row = await pg_store.get(task_id)
    assert row["status"] == "REJECTED"
    assert row["attempts"] == 0


@pytest.mark.integration
async def test_http_faststream_postgres_end_to_end(
    pg_store, isolated_publisher, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor

    from faststream import AckPolicy
    from faststream.rabbit import RabbitBroker
    from faststream.rabbit.schemas import Channel
    from httpx import ASGITransport, AsyncClient

    import issue_intelligence_platform.db.database as database
    import issue_intelligence_platform.main as main
    from issue_intelligence_platform.api.routes import health, process
    from issue_intelligence_platform.pipeline import worker
    from issue_intelligence_platform.pipeline.rabbit import work_queue

    monkeypatch.setattr(database, "engine", pg_store.sessions.kw["bind"])
    monkeypatch.setattr(main, "engine", pg_store.sessions.kw["bind"])
    monkeypatch.setattr(main, "session_factory", pg_store.sessions)
    monkeypatch.setattr(health, "engine", pg_store.sessions.kw["bind"])
    monkeypatch.setattr(process, "store", pg_store)
    monkeypatch.setattr(worker, "store", pg_store)
    monkeypatch.setattr(worker, "infer", lambda *a: "High")
    monkeypatch.setattr(worker.model_service, "model_version", "e2e-test")
    live = RabbitBroker(
        settings.rabbit_url,
        graceful_timeout=5,
        default_channel=Channel(prefetch_count=1),
    )
    live.subscriber(
        work_queue(),
        ack_policy=AckPolicy.MANUAL,
        decoder=worker.raw_decoder,
    )(worker.on_message)
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        try:
            await live.start()
            async with main.lifespan(main.app):
                async with AsyncClient(
                    transport=ASGITransport(app=main.app), base_url="http://test"
                ) as client:
                    assert (await client.get("/readyz")).status_code == 200
                    response = await client.post(
                        "/api/v1/tasks", json={"summary": "Fails"}
                    )
                    assert response.status_code == 202
                    url = response.json()["status_url"]
                    deadline = time.monotonic() + 15
                    while True:
                        result = (await client.get(url)).json()
                        if result["status"] == "SUCCEEDED":
                            break
                        assert result["status"] not in ("FAILED", "REJECTED")
                        if time.monotonic() > deadline:
                            raise TimeoutError(result)
                        await asyncio.sleep(0.05)
                    assert result["prediction"] == "High"
                    assert result["model_version"] == "e2e-test"
        finally:
            await live.stop()


@pytest.mark.integration
async def test_killed_worker_process_recovers_in_new_process(
    pg_store, isolated_publisher, postgres_url
):
    import os
    import socket
    import subprocess
    import sys

    task_id = uuid4()
    await accept_task(pg_store, isolated_publisher, task_id)
    code = """
import asyncio, time
from issue_intelligence_platform.pipeline import worker
def load():
    worker.model_service.model = object()
    worker.model_service.model_version = "crash-test"
worker.model_service.load = load
worker.infer = lambda *args: (time.sleep(worker.settings.inference_delay_seconds) or "High")
asyncio.run(worker.run())
"""
    base_env = {
        **os.environ,
        "DATABASE_URL": postgres_url,
        "RABBIT_URL": settings.rabbit_url,
        "RABBIT_QUEUE": settings.rabbit_queue,
        "RABBIT_DLQ": settings.rabbit_dlq,
        "RABBIT_DLX": settings.rabbit_dlx,
        "RABBIT_MAX_LENGTH": "20",
        "ENABLE_BACKPRESSURE": "true",
        "WORKER_CONCURRENCY": "1",
        "WORKER_PREFETCH_COUNT": "1",
        "WORKER_CLAIM_TIMEOUT_SECONDS": "1",
        "WORKER_HEARTBEAT_SECONDS": ".1",
        "WORKER_RETRY_DELAY_SECONDS": ".05",
    }
    children = []

    def start(delay):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", code],
            env={
                **base_env,
                "INFERENCE_DELAY_SECONDS": str(delay),
                "WORKER_HEALTH_PORT": str(port),
            },
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        children.append(child)
        return child

    async def wait_state(state, child):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            assert child.poll() is None, f"worker exited with {child.returncode}"
            row = await pg_store.get(task_id)
            if row["status"] == state:
                return row
            await asyncio.sleep(0.1)
        raise TimeoutError(f"worker did not reach {state}")

    try:
        first = start(15)
        await wait_state("PROCESSING", first)
        first.kill()
        await asyncio.to_thread(first.wait, timeout=10)
        assert (await pg_store.get(task_id))["status"] == "PROCESSING"
        second = start(0.01)
        row = await wait_state("SUCCEEDED", second)
        assert row["attempts"] == 2
        assert row["model_version"] == "crash-test"
        assert row["prediction"] == "High"
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            await asyncio.to_thread(child.wait, timeout=10)


@pytest.mark.integration
async def test_worker_process_sigterm_drains_active_inference(
    pg_store, isolated_publisher, postgres_url
):
    import os
    import socket
    import subprocess
    import sys

    task_id = uuid4()
    await accept_task(pg_store, isolated_publisher, task_id)
    code = """
import asyncio, signal, time
from issue_intelligence_platform.pipeline import worker
def load():
    worker.model_service.model = object()
    worker.model_service.model_version = "graceful-test"
def infer(*args):
    signal.raise_signal(signal.SIGTERM)
    time.sleep(.3)
    return "High"
worker.model_service.load = load
worker.infer = infer
asyncio.run(worker.run())
"""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "DATABASE_URL": postgres_url,
        "RABBIT_URL": settings.rabbit_url,
        "RABBIT_QUEUE": settings.rabbit_queue,
        "RABBIT_DLQ": settings.rabbit_dlq,
        "RABBIT_DLX": settings.rabbit_dlx,
        "RABBIT_MAX_LENGTH": "20",
        "ENABLE_BACKPRESSURE": "true",
        "WORKER_CONCURRENCY": "1",
        "WORKER_PREFETCH_COUNT": "1",
        "WORKER_CLAIM_TIMEOUT_SECONDS": "5",
        "WORKER_HEARTBEAT_SECONDS": ".1",
        "WORKER_HEALTH_PORT": str(port),
    }
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", code],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert await asyncio.to_thread(child.wait, timeout=45) == 0
        row = await pg_store.get(task_id)
        assert row["status"] == "SUCCEEDED"
        assert row["attempts"] == 1
        assert row["model_version"] == "graceful-test"
    finally:
        if child.poll() is None:
            child.kill()
        await asyncio.to_thread(child.wait, timeout=10)


@pytest.mark.integration
async def test_lost_confirm_cancels_real_message_without_inference(
    pg_store, isolated_publisher, monkeypatch
):
    from issue_intelligence_platform.pipeline import worker
    from issue_intelligence_platform.pipeline.admission import submit
    from issue_intelligence_platform.pipeline.rabbit import PublishUncertain

    pub = isolated_publisher

    class LostConfirm:
        async def publish(self, task_id, summary, description):
            await pub.publish(task_id, summary, description)
            raise PublishUncertain("simulated lost confirmation")

    task_id = uuid4()
    with pytest.raises(PublishUncertain):
        await submit(pg_store, LostConfirm(), task_id, "Fails", "")
    monkeypatch.setattr(worker, "store", pg_store)
    queue = await pub.channel.declare_queue(settings.rabbit_queue, passive=True)
    msg = await get_delivery(queue)
    await worker.handle_task(msg.body, worker_delivery(msg))
    row = await pg_store.get(task_id)
    assert row["status"] == "REJECTED"
    assert row["attempts"] == 0
    assert row["prediction"] is None
    assert await queue.get(fail=False) is None


@pytest.mark.integration
async def test_producer_crash_after_confirm_is_reconciled(
    pg_store, isolated_publisher, monkeypatch
):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from issue_intelligence_platform.pipeline import worker
    from issue_intelligence_platform.pipeline.store import Task

    pub = isolated_publisher
    task_id = uuid4()
    await pg_store.stage(task_id, "Fails", "")
    await pub.publish(task_id, "Fails", "")
    # Producer disappeared before SQL PENDING. Simulate elapsed timeout only.
    async with pg_store.sessions.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == str(task_id))
            .values(updated_at=datetime.now(UTC) - timedelta(seconds=60))
        )
    assert await pg_store.expire_publications(30) == 1
    monkeypatch.setattr(worker, "store", pg_store)
    queue = await pub.channel.declare_queue(settings.rabbit_queue, passive=True)
    msg = await get_delivery(queue)
    await worker.handle_task(msg.body, worker_delivery(msg))
    assert (await pg_store.get(task_id))["status"] == "REJECTED"
    assert (await pg_store.get(task_id))["attempts"] == 0
    assert await queue.get(fail=False) is None

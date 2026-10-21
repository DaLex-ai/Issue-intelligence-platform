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


@pytest.fixture
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

import asyncio
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from issue_intelligence_platform.pipeline import worker
from issue_intelligence_platform.pipeline.rabbit import (
    ConfirmingPublisher,
    PublishUncertain,
)
from issue_intelligence_platform.pipeline.reconciliation import reconcile_loop
from issue_intelligence_platform.pipeline.store import Claim


async def test_publish_timeout_includes_waiting_for_reconnect(monkeypatch):
    async def blocked(*args, **kwargs):
        await asyncio.Event().wait()

    pub = ConfirmingPublisher()
    pub.channel = SimpleNamespace(default_exchange=SimpleNamespace(publish=blocked))
    monkeypatch.setattr(worker.settings, "publisher_confirm_timeout", 0.01)
    with pytest.raises(PublishUncertain):
        await asyncio.wait_for(pub.publish(uuid4(), "Fails", ""), timeout=0.5)


async def test_reconciler_survives_database_failure(monkeypatch):
    event = asyncio.Event()
    calls = 0

    async def expire(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("database down")
        event.set()
        return 1

    monkeypatch.setattr(worker.settings, "publication_reconcile_seconds", 0.001)
    task = asyncio.create_task(
        reconcile_loop(SimpleNamespace(expire_publications=expire))
    )
    try:
        await asyncio.wait_for(event.wait(), timeout=1)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    assert calls >= 2


async def test_api_startup_failure_closes_publisher_and_engine(monkeypatch):
    import issue_intelligence_platform.main as main

    pub = SimpleNamespace(
        start=AsyncMock(side_effect=OSError("broker down")), close=AsyncMock()
    )
    engine = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(main, "initialize_schema", AsyncMock())
    monkeypatch.setattr(main, "engine", engine)
    monkeypatch.setattr(main, "ConfirmingPublisher", lambda: pub)
    with pytest.raises(OSError):
        async with main.lifespan(main.app):
            pytest.fail("startup should fail")
    pub.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_worker_readiness_checks_dependencies_and_content_length(monkeypatch):
    @asynccontextmanager
    async def connection():
        yield SimpleNamespace(execute=AsyncMock())

    monkeypatch.setattr(worker, "engine", SimpleNamespace(connect=connection))
    monkeypatch.setattr(worker, "executor", object())
    monkeypatch.setattr(worker.model_service, "model", object())
    ping = AsyncMock(return_value=True)
    monkeypatch.setattr(worker.broker, "ping", ping)
    server = await asyncio.start_server(worker.health_handler, "127.0.0.1", 0)

    async def response():
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=3)
            headers, body = raw.split(b"\r\n\r\n", 1)
            length = next(
                line
                for line in headers.split(b"\r\n")
                if line.startswith(b"Content-Length:")
            )
            assert int(length.split(b":")[1]) == len(body)
            return headers
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        assert b"200 OK" in await response()
        ping.return_value = False
        assert b"503 Service Unavailable" in await response()
        ping.return_value = True
        monkeypatch.setattr(worker, "executor", None)
        assert b"503 Service Unavailable" in await response()
    finally:
        server.close()
        await server.wait_closed()


async def test_worker_nacks_when_result_and_failure_storage_are_unavailable(
    monkeypatch,
):
    from concurrent.futures import ThreadPoolExecutor

    store = AsyncMock()
    store.claim.return_value = Claim("claimed", 1, "Fails", "")
    store.succeed.side_effect = OSError("database down")
    store.fail.side_effect = OSError("still down")
    monkeypatch.setattr(worker, "store", store)
    monkeypatch.setattr(worker, "infer", lambda *args: "High")
    monkeypatch.setattr(worker.settings, "worker_retry_delay_seconds", 0.001)
    msg = SimpleNamespace(
        raw_message=SimpleNamespace(redelivered=False),
        ack=AsyncMock(),
        nack=AsyncMock(),
        reject=AsyncMock(),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    msg.nack.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()
    msg.reject.assert_not_awaited()

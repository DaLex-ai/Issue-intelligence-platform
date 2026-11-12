"""Consumer tests use TestRabbitBroker, with SQL and ML isolated."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock
from uuid import uuid4

from faststream.rabbit import TestRabbitBroker

from issue_intelligence_platform.pipeline import worker
from issue_intelligence_platform.pipeline.store import Claim


class Delivery:
    def __init__(self, redelivered=False):
        self.raw_message = type("Raw", (), {"redelivered": redelivered})()
        self.ack = AsyncMock()
        self.nack = AsyncMock()
        self.reject = AsyncMock()


async def test_success_commits_result_before_ack(monkeypatch):
    events = []
    fake_store = AsyncMock()
    fake_store.claim.return_value = Claim("claimed", 1, "Fails", "")
    fake_store.succeed.side_effect = lambda *a, **kw: events.append("commit") or True
    msg = Delivery()
    msg.ack.side_effect = lambda: events.append("ack")
    monkeypatch.setattr(worker, "store", fake_store)
    monkeypatch.setattr(worker, "infer", lambda *a: "Urgent")
    with ThreadPoolExecutor(max_workers=2) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    assert events == ["commit", "ack"]
    msg.reject.assert_not_awaited()


async def test_bad_payload_is_rejected_to_dlq():
    msg = Delivery()
    await worker.handle_task({"summary": "missing task id"}, msg)
    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


async def test_permanent_error_reaches_dlq(monkeypatch):
    fake_store = AsyncMock()
    fake_store.claim.return_value = Claim("claimed", 1, "Fails", "")
    fake_store.fail.return_value = True
    monkeypatch.setattr(worker, "store", fake_store)
    monkeypatch.setattr(worker, "infer", lambda *a: 1 / 0)
    msg = Delivery()
    with ThreadPoolExecutor(max_workers=2) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    fake_store.fail.assert_awaited_once()
    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


async def test_redelivery_recovers_then_acks_terminal(monkeypatch):
    fake_store = AsyncMock()
    fake_store.claim.return_value = Claim("terminal")
    monkeypatch.setattr(worker, "store", fake_store)
    msg = Delivery(redelivered=True)
    await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    assert fake_store.claim.call_args.kwargs["redelivered"] is True
    msg.ack.assert_awaited_once()


async def test_faststream_test_rabbit_broker_invokes_handler(monkeypatch):
    spy = AsyncMock()
    monkeypatch.setattr(worker, "handle_task", spy)
    async with TestRabbitBroker(worker.broker):
        await worker.broker.publish(
            {"task_id": str(uuid4()), "summary": "From broker"},
            queue=worker.work_queue(),
        )
    spy.assert_awaited_once()


async def test_corrupt_json_bytes_are_dead_lettered():
    msg = Delivery()
    await worker.handle_task(b"{bad-json", msg)
    msg.reject.assert_awaited_once_with(requeue=False)


async def test_failed_redelivery_rejects_after_failure_was_committed(monkeypatch):
    fake_store = AsyncMock()
    fake_store.claim.return_value = Claim("failed")
    monkeypatch.setattr(worker, "store", fake_store)
    msg = Delivery(redelivered=True)
    await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


async def test_database_unavailable_during_claim_requeues(monkeypatch):
    fake_store = AsyncMock()
    fake_store.claim.side_effect = OSError("database unavailable")
    monkeypatch.setattr(worker, "store", fake_store)
    monkeypatch.setattr(worker.settings, "worker_retry_delay_seconds", 0.001)
    msg = Delivery(redelivered=True)
    await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    msg.nack.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()
    fake_store.fail.assert_not_awaited()


async def test_stale_result_is_not_acknowledged(monkeypatch):
    fake_store = AsyncMock()
    fake_store.claim.return_value = Claim("claimed", 7, "Fails", "")
    fake_store.succeed.return_value = False
    monkeypatch.setattr(worker, "store", fake_store)
    monkeypatch.setattr(worker, "infer", lambda *a: "High")
    monkeypatch.setattr(worker.settings, "worker_retry_delay_seconds", 0.001)
    msg = Delivery()
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(worker, "executor", executor)
        await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    assert fake_store.succeed.call_args.kwargs["attempt"] == 7
    msg.nack.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()


async def test_publishing_and_live_duplicate_are_deferred(monkeypatch):
    monkeypatch.setattr(worker.settings, "worker_retry_delay_seconds", 0.001)
    for decision in ("publishing", "busy"):
        fake_store = AsyncMock()
        fake_store.claim.return_value = Claim(decision)
        monkeypatch.setattr(worker, "store", fake_store)
        msg = Delivery()
        await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

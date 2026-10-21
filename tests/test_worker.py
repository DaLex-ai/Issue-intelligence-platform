"""Consumer tests use TestRabbitBroker, with SQL and ML isolated."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock
from uuid import uuid4

from faststream.rabbit import TestRabbitBroker

from issue_intelligence_platform.pipeline import worker


class Delivery:
    def __init__(self, redelivered=False):
        self.raw_message = type("Raw", (), {"redelivered": redelivered})()
        self.ack = AsyncMock()
        self.nack = AsyncMock()
        self.reject = AsyncMock()


async def test_success_commits_result_before_ack(monkeypatch):
    events = []
    fake_store = AsyncMock()
    fake_store.claim.return_value = "claimed"
    fake_store.succeed.side_effect = lambda *a: events.append("commit")
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
    fake_store.claim.return_value = "claimed"
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
    fake_store.claim.return_value = "terminal"
    monkeypatch.setattr(worker, "store", fake_store)
    msg = Delivery(redelivered=True)
    await worker.handle_task({"task_id": str(uuid4()), "summary": "Fails"}, msg)
    fake_store.recover.assert_awaited_once()
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

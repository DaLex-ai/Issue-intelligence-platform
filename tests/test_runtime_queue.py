"""Prevent destructive checks while RabbitMQ is still awaiting acknowledgements."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_runtime.py"
spec = importlib.util.spec_from_file_location("runtime_queue_checks", SCRIPT)
runtime = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(runtime)


def test_clean_queue_waits_for_pending_acks(monkeypatch):
    samples = [
        {"messages_ready": 0, "messages_unacknowledged": 2, "consumers": 1},
        {"messages_ready": 0, "messages_unacknowledged": 1, "consumers": 1},
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1},
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1},
    ]
    monkeypatch.setattr(runtime, "queue_info", lambda: samples.pop(0))
    monkeypatch.setattr(runtime.time, "sleep", lambda _secs: None)
    result = runtime.clean_queue(timeout=10)
    assert result["messages_unacknowledged"] == 0
    assert not samples


def test_clean_queue_never_purges_stuck_messages(monkeypatch):
    monkeypatch.setattr(
        runtime,
        "queue_info",
        lambda: {"messages_ready": 0, "messages_unacknowledged": 2, "consumers": 1},
    )
    with pytest.raises(RuntimeError, match="refusing to disrupt tasks"):
        runtime.clean_queue(timeout=0)


def test_clean_queue_requires_consumer(monkeypatch):
    monkeypatch.setattr(
        runtime,
        "queue_info",
        lambda: {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 0},
    )
    with pytest.raises(RuntimeError, match="No RabbitMQ consumer"):
        runtime.clean_queue(timeout=0)


def test_clean_queue_waits_for_consumer_registration(monkeypatch):
    samples = [
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 0},
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 0},
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1},
        {"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1},
    ]
    monkeypatch.setattr(runtime, "queue_info", lambda: samples.pop(0))
    monkeypatch.setattr(runtime.time, "sleep", lambda _secs: None)
    result = runtime.clean_queue(timeout=10)
    assert result["consumers"] == 1
    assert not samples


def test_failed_stage_writes_partial_report(monkeypatch, tmp_path):
    import json
    import sys

    monkeypatch.setattr(runtime, "ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["verify_runtime.py", "--mode", "all", "--allow-disruption"]
    )
    monkeypatch.setattr(runtime, "safe", lambda: {"task_status": "SUCCEEDED"})

    def fail_overload():
        raise RuntimeError("broker unavailable")

    monkeypatch.setattr(runtime, "overload", fail_overload)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        runtime.main()
    report = json.loads(
        (tmp_path / "results" / "performance" / "runtime_validation.json").read_text()
    )
    assert report["safe"]["task_status"] == "SUCCEEDED"
    assert report["failed_stage"] == "overload"
    assert "broker unavailable" in report["error"]

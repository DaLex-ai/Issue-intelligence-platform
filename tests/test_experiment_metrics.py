"""Regression checks for experiment bookkeeping; no Docker services needed."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "experiment.py"
spec = importlib.util.spec_from_file_location("pipeline_experiment", SCRIPT)
experiment = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(experiment)


def run_record(label="bounded_1_worker"):
    samples = [
        {
            "seconds": 0,
            "ready": 0,
            "unacked": 0,
            "ack_count": 0,
            "db_succeeded": 0,
            "memory_mib": {},
        },
        {
            "seconds": 8,
            "ready": 20,
            "unacked": 2,
            "ack_count": 20,
            "db_succeeded": 20,
            "memory_mib": {},
        },
        {
            "seconds": 16,
            "ready": 20,
            "unacked": 2,
            "ack_count": 52,
            "db_succeeded": 52,
            "memory_mib": {},
        },
        {
            "seconds": 24,
            "ready": 20,
            "unacked": 2,
            "ack_count": 84,
            "db_succeeded": 84,
            "memory_mib": {},
        },
        {
            "seconds": 33,
            "ready": 0,
            "unacked": 0,
            "ack_count": 100,
            "db_succeeded": 100,
            "memory_mib": {},
        },
    ]
    events = [{"event": "publish_confirm", "seconds": 8 + i / 4} for i in range(65)]
    return {
        "label": label,
        "duration": 33.5,
        "limit": 20,
        "workers": 1,
        "tasks": 100,
        "completed": 100,
        "completed_db": 100,
        "rejected": 30,
        "latencies_seconds": [[0.5, 3.0]] * 100,
        "samples": samples,
        "events": events,
    }


def test_completed_scenario_requires_drain_and_all_rows():
    experiment.verify_run(run_record())


def test_missing_result_is_not_silently_accepted():
    run = run_record()
    run["completed_db"] = 99
    with pytest.raises(ValueError, match="Incomplete"):
        experiment.verify_run(run)


def test_nonzero_unacknowledged_final_state_is_rejected():
    run = run_record()
    run["samples"][-1]["unacked"] = 1
    with pytest.raises(ValueError, match="not drained"):
        experiment.verify_run(run)


def test_saturated_rate_excludes_warmup_and_drain():
    rates = experiment.saturated_rates(run_record())
    assert rates is not None
    assert rates["type"] == "saturated sampled interval"
    assert 0 < rates["source"] < 10
    assert rates["worker"] == 4


def test_baseline_is_not_presented_as_steady_state():
    run = run_record("baseline")
    rates = experiment.saturated_rates(run)
    assert rates is not None
    assert rates["worker"] is None
    assert "not steady" in rates["type"]


def test_docker_memory_ignores_unrelated_compose_projects(monkeypatch):
    class FakeResult:
        stdout = (
            "issue-intelligence-platform-worker-2|274MiB / 5GiB\n"
            "issue-intelligence-platform-batch-run-a|60MiB / 5GiB\n"
            "another-project-worker-1|900MiB / 5GiB\n"
        )

    monkeypatch.setattr(experiment.subprocess, "run", lambda *a, **kw: FakeResult())
    monkeypatch.setattr(experiment, "PROJECT", "issue-intelligence-platform")
    memory = experiment.memory_mib()
    assert sum(memory.values()) == 334
    assert len(memory) == 2


def test_visualizations_are_generated_without_actual_services(tmp_path, monkeypatch):
    monkeypatch.setattr(experiment, "RESULTS", tmp_path)
    runs = [run_record("baseline"), run_record(), run_record("bounded_2_workers")]
    experiment.plot(runs)
    assert (tmp_path / "queue_total.png").exists()
    assert (tmp_path / "db_completed.png").exists()
    assert (tmp_path / "pipeline_memory.png").exists()

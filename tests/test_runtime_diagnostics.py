"""Read-only regression tests for actionable runtime diagnostics."""

import importlib.util
import subprocess
import urllib.error
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_runtime.py"
spec = importlib.util.spec_from_file_location("runtime_validation", SCRIPT)
runtime = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(runtime)


def test_connection_refused_has_actionable_message(monkeypatch):
    def fail(*_args, **_kwargs):
        raise urllib.error.URLError(ConnectionRefusedError("Connection refused"))

    monkeypatch.setattr(runtime.urllib.request, "urlopen", fail)
    with pytest.raises(RuntimeError, match="docker compose ps"):
        runtime.request("http://127.0.0.1:8000/healthz")


def test_docker_error_reports_stderr(monkeypatch):
    def fail(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            returncode=1, cmd=["docker"], stderr="worker is not running"
        )

    monkeypatch.setattr(runtime.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="worker is not running"):
        runtime.docker("exec", "-T", "worker", "python", "-V")


def test_safe_checks_worker_existence_before_request(monkeypatch):
    monkeypatch.setattr(
        runtime,
        "worker_count",
        lambda: (_ for _ in ()).throw(RuntimeError("Worker is not running")),
    )
    with pytest.raises(RuntimeError, match="Worker is not running"):
        runtime.safe()

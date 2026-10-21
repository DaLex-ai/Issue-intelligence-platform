"""The inference timer measures execution in executor, not executor queuing."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from issue_intelligence_platform.pipeline import worker


def test_timed_inference_does_not_include_executor_wait(monkeypatch):
    monkeypatch.setattr(worker, "infer", lambda summary, description: "Normal")
    gate = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        blocking = executor.submit(gate.wait, 2)
        started = time.perf_counter()
        future = executor.submit(worker.timed_infer, "summary", "description")
        time.sleep(0.08)
        assert not future.done()
        gate.set()
        blocking.result(timeout=2)
        prediction, seconds = future.result(timeout=2)
    assert prediction == "Normal"
    assert time.perf_counter() - started >= 0.08
    assert 0 <= seconds < 0.08

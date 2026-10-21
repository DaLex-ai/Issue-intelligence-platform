"""Operational checks for a local, isolated Issue Intelligence Compose stack.

Safe smoke test:
    uv run python scripts/verify_runtime.py --mode safe
Disruptive checks (stop/restart workers and enqueue synthetic tasks):
    uv run python scripts/verify_runtime.py --mode all --allow-disruption

Never deletes queues, database rows, or Docker volumes. Disruptive checks require
an initially empty work queue. Test only on an isolated development stack.
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
API = os.environ.get("INFERENCE_API_URL", "http://127.0.0.1:8000")
MANAGEMENT = os.environ.get("RABBIT_MANAGEMENT_URL", "http://127.0.0.1:15672")


def docker(*args, env=None, timeout=200):
    try:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker CLI unavailable; start Docker Desktop") from exc
    except subprocess.CalledProcessError as exc:
        details = (exc.stderr or exc.stdout or "no command output").strip()[-1200:]
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed (exit {exc.returncode}): {details}"
        ) from exc


def request(url, method="GET", data=None, management=False):
    headers = {}
    if management:
        user = os.environ.get("RABBIT_MANAGEMENT_USER", "guest")
        password = os.environ.get("RABBIT_MANAGEMENT_PASSWORD", "guest")
        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = "Basic " + credentials
    if data is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=8) as response:
            return response.status, dict(response.headers), json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            body = json.load(exc)
        except ValueError:
            body = {}
        return exc.code, dict(exc.headers), body
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
        raise RuntimeError(
            f"Cannot connect to {url}: {exc}. "
            "Check docker compose ps; start services using "
            "docker compose up -d --wait app worker"
        ) from exc


def queue_info(name="issue.inference.tasks"):
    url = f"{MANAGEMENT}/api/queues/%2F/{name}"
    status, _, data = request(url, management=True)
    if status != 200:
        raise RuntimeError(f"Cannot read RabbitMQ queue {name}: HTTP {status}: {data}")
    return data


def clean_queue(timeout=90):
    """Wait for a healthy consumer and drained queue; never purge messages.

    RabbitMQ management's consumer count may briefly read zero after a
    docker-compose restart. Require two empty observations *with* a consumer.
    A persistent zero is a failure, not permission to disrupt an active stack.
    """
    deadline = time.monotonic() + timeout
    empty_observations = 0
    next_notice = time.monotonic() + 10
    while True:
        q = queue_info()
        ready = int(q.get("messages_ready") or 0)
        unacked = int(q.get("messages_unacknowledged") or 0)
        consumers = int(q.get("consumers") or 0)
        if consumers >= 1 and ready == 0 and unacked == 0:
            empty_observations += 1
            if empty_observations >= 2:
                return q
        else:
            empty_observations = 0

        now = time.monotonic()
        if now >= deadline:
            if consumers < 1:
                raise RuntimeError(
                    "No RabbitMQ consumer available after waiting "
                    f"{timeout}s: ready={ready}, unacked={unacked}, "
                    f"consumers={consumers}. Inspect 'docker compose ps worker' "
                    "and 'docker compose logs --tail=80 worker'. "
                    "To restore the consumer run: "
                    "docker compose up -d --wait --scale worker=1 worker"
                )
            raise RuntimeError(
                "Queue did not drain in time; refusing to disrupt tasks: "
                f"ready={ready}, unacked={unacked}, consumers={consumers}. "
                "Inspect docker compose logs worker and RabbitMQ Management UI."
            )
        if now >= next_notice:
            print(
                "[runtime] waiting for RabbitMQ to become idle: "
                f"ready={ready}, unacked={unacked}, consumers={consumers}",
                flush=True,
            )
            next_notice = now + 10
        time.sleep(0.5)


def task_post(summary):
    return request(
        f"{API}/api/v1/tasks",
        method="POST",
        data={"summary": summary, "description": "Automated runtime verification"},
    )


def wait_tasks(task_ids, timeout=120):
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        completed = True
        for task_id in task_ids:
            status, _, data = request(f"{API}/api/v1/tasks/{task_id}")
            if status != 200:
                raise RuntimeError(f"Cannot read task {task_id}: HTTP {status}")
            last[task_id] = data
            if data["status"] in ("FAILED", "REJECTED"):
                raise RuntimeError(f"Task {task_id} failed: {data}")
            if data["status"] != "SUCCEEDED":
                completed = False
        if completed:
            return last
        time.sleep(0.5)
    raise TimeoutError(f"Tasks not completed in time: {last}")


def worker_count():
    result = docker("ps", "-q", "worker")
    count = len([item for item in result.stdout.splitlines() if item.strip()])
    if count < 1:
        raise RuntimeError("Worker is not running")
    return count


def restore_workers(count, env=None):
    docker(
        "up",
        "-d",
        "--wait",
        "--scale",
        f"worker={count}",
        "worker",
        env=env,
        timeout=240,
    )


def check_worker_health():
    cmd = (
        "import urllib.request; "
        "print(urllib.request.urlopen('http://127.0.0.1:8081/',timeout=3).status)"
    )
    result = docker("exec", "-T", "worker", "python", "-c", cmd, timeout=15)
    if result.stdout.strip() != "200":
        raise AssertionError(f"Worker health failed: {result.stdout}")


def safe():
    worker_count()  # Fail clearly if the worker was never started.
    status, _, api = request(f"{API}/healthz")
    if status != 200:
        raise AssertionError(f"API unhealthy: {status}, {api}")
    check_worker_health()
    status, _, reply = task_post("Runtime smoke " + uuid4().hex)
    if status != 202:
        raise AssertionError(f"Expected 202, got HTTP {status}: {reply}")
    task_id = reply["task_id"]
    final = wait_tasks([task_id])[task_id]
    if not final.get("prediction") or not final.get("model_version"):
        raise AssertionError(f"Missing model prediction/version: {final}")
    return {
        "api_health": 200,
        "worker_health": 200,
        "task_id": task_id,
        "task_status": "SUCCEEDED",
        "model_version": final["model_version"],
    }


def overload():
    q = clean_queue()
    queue_limit = (q.get("arguments") or {}).get("x-max-length")
    if not isinstance(queue_limit, int) or queue_limit > 100 or queue_limit < 1:
        raise RuntimeError(
            f"Overload test needs bounded queue 1..100, got {queue_limit}"
        )
    count = worker_count()
    local_env = dotenv_values(ROOT / ".env")
    db_user = (
        os.environ.get("POSTGRES_USER") or local_env.get("POSTGRES_USER") or "issue_app"
    )
    db_name = (
        os.environ.get("POSTGRES_DB")
        or local_env.get("POSTGRES_DB")
        or "issue_intelligence"
    )
    accepted, refused, retries = [], 0, []
    run_id = uuid4().hex[:12]
    docker("stop", "worker")
    try:
        for index in range(queue_limit + 4):
            code, headers, body = task_post(f"Runtime overload {run_id} {index}")
            if code == 202:
                accepted.append(body["task_id"])
            elif code == 503:
                refused += 1
                h = {key.lower(): val for key, val in headers.items()}
                retry = h.get("retry-after")
                if retry is None or not retry.isdecimal() or int(retry) < 1:
                    raise AssertionError(f"Missing/invalid Retry-After: {headers}")
                retries.append(int(retry))
            else:
                raise AssertionError(f"Unexpected HTTP {code}: {body}")
        if not refused or len(accepted) != queue_limit:
            raise AssertionError(
                f"Expected {queue_limit} accepted and >=1 refused; "
                f"got accepted={len(accepted)}, refused={refused}"
            )
        # No rejected request is left as PENDING: only accepted IDs can be pending.
        db = docker(
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            db_user,
            "-d",
            db_name,
            "-At",
            "-c",
            "SELECT count(*) FROM inference_tasks WHERE summary LIKE "
            f"'Runtime overload {run_id} %' AND state='PENDING'",
        )
        if int(db.stdout.strip()) != len(accepted):
            raise AssertionError("Refused HTTP requests left unexpected PENDING rows")
    finally:
        restore_workers(count)
    wait_tasks(accepted, timeout=max(120, queue_limit * 5))
    clean_queue(timeout=max(60, queue_limit * 5))
    return {
        "accepted": len(accepted),
        "refused_503": refused,
        "retry_after_seconds": sorted(set(retries)),
        "completed": len(accepted),
    }


def dlq():
    clean_queue()
    before = queue_info("issue.inference.dlq").get("messages_ready", 0)
    bad_message = {
        "properties": {"delivery_mode": 2},
        "routing_key": "issue.inference.tasks",
        "payload": "bad-not-json-" + uuid4().hex,
        "payload_encoding": "string",
    }
    code, _, result = request(
        f"{MANAGEMENT}/api/exchanges/%2F/amq.default/publish",
        method="POST",
        data=bad_message,
        management=True,
    )
    if code != 200 or not result.get("routed"):
        raise RuntimeError(f"Cannot publish test message to working queue: {result}")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        after = queue_info("issue.inference.dlq").get("messages_ready", 0)
        if after > before:
            return {"dlq_before": before, "dlq_after": after}
        time.sleep(0.5)
    raise TimeoutError("Invalid message did not appear in DLQ")


def shutdown(kind):
    clean_queue()
    count = worker_count()
    env = {**os.environ, "INFERENCE_DELAY_SECONDS": "3"}
    task_ids = []
    observed_inflight = False
    try:
        docker(
            "up", "-d", "--wait", "--scale", "worker=1", "worker", env=env, timeout=240
        )
        for index in range(12):
            status, _, reply = task_post(
                f"Runtime shutdown {kind} {uuid4().hex} {index}"
            )
            if status != 202:
                raise RuntimeError(f"Unable to queue shutdown task: {status} {reply}")
            task_ids.append(reply["task_id"])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if queue_info().get("messages_unacknowledged", 0) > 0:
                observed_inflight = True
                break
            time.sleep(0.3)
        if not observed_inflight:
            raise RuntimeError("No in-flight work observed; cannot verify shutdown")
        check_worker_health()  # Nonblocking event loop while inference is running.
        if kind == "sigterm":
            docker("stop", "worker", timeout=100)
        else:
            docker("kill", "-s", "SIGKILL", "worker", timeout=20)
        status_after_stop = {}
        for task_id in task_ids:
            code, _, result = request(f"{API}/api/v1/tasks/{task_id}")
            if code != 200:
                raise AssertionError(f"Task vanished after {kind}: {task_id}")
            status_after_stop[result["status"]] = (
                status_after_stop.get(result["status"], 0) + 1
            )
        if kind == "sigterm" and status_after_stop.get("PROCESSING", 0):
            raise AssertionError(f"SIGTERM abandoned active tasks: {status_after_stop}")
    finally:
        restore_workers(count)
    wait_tasks(task_ids, timeout=180)
    return {
        "signal": kind,
        "inflight_observed": observed_inflight,
        "health_during_inference": 200,
        "states_after_stop": status_after_stop,
        "eventual_succeeded": len(task_ids),
    }


def save_results(results):
    location = ROOT / "results" / "performance" / "runtime_validation.json"
    location.parent.mkdir(parents=True, exist_ok=True)
    data = {**results, "measured_at_utc": datetime.now(UTC).isoformat()}
    location.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return location


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("safe", "overload", "dlq", "graceful", "crash", "all"),
        default="safe",
    )
    parser.add_argument("--allow-disruption", action="store_true")
    args = parser.parse_args()
    if args.mode != "safe" and not args.allow_disruption:
        parser.error("Disruptive checks require --allow-disruption")

    stages = (
        ("safe", safe),
        ("overload", overload),
        ("dlq", dlq),
        ("graceful", lambda: shutdown("sigterm")),
        ("crash", lambda: shutdown("sigkill")),
    )
    results = {}
    for label, check in stages:
        if args.mode not in ("all", label):
            continue
        print(f"[runtime] START {label}", flush=True)
        try:
            results[label] = check()
        except Exception as exc:
            # Save what genuinely passed rather than leaving a stale 'safe'
            # report from an earlier run looking like a successful full check.
            results["failed_stage"] = label
            results["error"] = f"{type(exc).__name__}: {exc}"
            location = save_results(results)
            print(f"[runtime] FAILED {label}; partial report: {location}", flush=True)
            raise
        save_results(results)
        print(f"[runtime] PASS {label}", flush=True)

    location = save_results(results)
    print(
        json.dumps(
            {**results, "measured_at_utc": datetime.now(UTC).isoformat()},
            indent=2,
            ensure_ascii=False,
        )
    )
    print("Verified checks saved to", location)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, AssertionError, TimeoutError) as exc:
        print(f"RUNTIME VERIFICATION FAILED: {exc}", file=sys.stderr)
        sys.exit(1)

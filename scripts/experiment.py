"""Reproducible RabbitMQ inference benchmark with verified completion.

Run ONLY on an isolated development Compose project without unrelated tasks:
    uv run python scripts/experiment.py --tasks 240 --period 1

Never removes database volumes. Redeclares a work queue only when it has no
messages or consumers; does not delete the DLQ. Requires a registered model.
"""

import argparse
import base64
import json
import os
import re
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "performance"
RABBIT_API = os.environ.get(
    "RABBIT_MANAGEMENT_API",
    "http://127.0.0.1:15672/api/queues/%2F/issue.inference.tasks",
)
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", ROOT.name.lower().replace("_", "-"))


def docker(*args: str, env=None, timeout=120):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def rabbit(method="GET"):
    credentials = (
        os.environ.get("RABBIT_MANAGEMENT_USER", "guest")
        + ":"
        + os.environ.get("RABBIT_MANAGEMENT_PASSWORD", "guest")
    ).encode("utf-8")
    request = urllib.request.Request(
        RABBIT_API,
        method=method,
        headers={"Authorization": "Basic " + base64.b64encode(credentials).decode()},
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return {} if method == "DELETE" else json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def memory_mib():
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    factors = {
        "B": 1 / 2**20,
        "KiB": 1 / 1024,
        "MiB": 1,
        "GiB": 1024,
        "kB": 1 / 1000,
        "MB": 1,
        "GB": 1000,
    }
    output = {}
    for line in result.stdout.splitlines():
        if "|" not in line:
            continue
        name, usage = line.split("|", 1)
        # Exclude every other Docker Compose project on this computer.
        if not name.startswith(f"{PROJECT}-"):
            continue
        if "-worker-" not in name and "-batch-run-" not in name:
            continue
        match = re.match(r"\s*([\d.]+)([A-Za-z]+)", usage)
        if match:
            output[name] = round(float(match.group(1)) * factors[match.group(2)], 2)
    return output


def create_input(label, tasks, run_id):
    path = ROOT / "data" / f"perf_{label}_{run_id}.jsonl"
    path.parent.mkdir(exist_ok=True)
    with path.open("w", encoding="utf-8") as target:
        for i in range(tasks):
            target.write(
                json.dumps(
                    {
                        "summary": (
                            f"Performance {label} {run_id} issue {i}: "
                            "storage node fails during restart"
                        ),
                        "description": (
                            f"Reproducible cluster log error in shard {i % 17}."
                        ),
                    }
                )
                + "\n"
            )
    return path


def wait_for_queue(predicate, description, timeout, allow_missing=False):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        queue = rabbit()
        if queue is None and allow_missing:
            return None
        if queue is not None and predicate(queue):
            return queue
        time.sleep(1)
    q = rabbit()
    raise TimeoutError(f"Timed out waiting for {description}; queue={q}")


def prepare_queue(env):
    docker("up", "-d", "rabbitmq", env=env)
    # Stop external HTTP admissions; keep consumers alive until accepted work drains.
    docker("stop", "app", env=env)
    q = rabbit()
    if q is not None and q.get("messages", 0):
        if q.get("consumers", 0) == 0:
            raise RuntimeError("Queue is nonempty but no consumers can drain it")
        wait_for_queue(
            lambda item: (
                item.get("messages_ready", 0) == 0
                and item.get("messages_unacknowledged", 0) == 0
            ),
            "existing work to finish",
            timeout=180,
        )
    docker("stop", "worker", env=env)
    wait_for_queue(
        lambda item: (
            item.get("messages_ready", 0) == 0
            and item.get("messages_unacknowledged", 0) == 0
            and item.get("consumers", 0) == 0
        ),
        "queue to become empty and consumer-free",
        timeout=75,
        allow_missing=True,
    )
    if rabbit() is not None:
        rabbit("DELETE")  # Empty work queue ONLY. Never delete DLQ or database data.
    docker(
        "up",
        "-d",
        "--wait",
        "--scale",
        "worker=1",
        "app",
        "worker",
        env=env,
        timeout=240,
    )
    wait_for_queue(lambda item: item.get("consumers", 0) >= 1, "worker", 120)


def database_states(label, run_id, env, db_user, db_name):
    sql = (
        "SELECT state, COUNT(*) FROM inference_tasks WHERE summary LIKE "
        f"'Performance {label} {run_id} %' GROUP BY state"
    )
    result = docker(
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
        sql,
        env=env,
        timeout=30,
    )
    states = {}
    for line in result.stdout.splitlines():
        if "|" in line:
            key, count = line.split("|", 1)
            states[key] = int(count)
    return states


def db_timings(label, run_id, env, db_user, db_name):
    sql = (
        "SELECT COALESCE(inference_seconds::text,'') || ',' || "
        "EXTRACT(EPOCH FROM (updated_at - created_at))::text "
        "FROM inference_tasks WHERE summary LIKE "
        f"'Performance {label} {run_id} %' AND state = 'SUCCEEDED'"
    )
    result = docker(
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
        sql,
        env=env,
        timeout=30,
    )
    return [
        [float(part) for part in line.split(",")]
        for line in result.stdout.splitlines()
        if "," in line and all(line.split(","))
    ]


def make_sample(t0, q, states):
    return {
        "seconds": round(time.time() - t0, 2),
        "ready": q.get("messages_ready", 0),
        "unacked": q.get("messages_unacknowledged", 0),
        "ack_count": (q.get("message_stats") or {}).get("ack", 0),
        "memory_mib": memory_mib(),
        "db_succeeded": states.get("SUCCEEDED", 0),
    }


def verify_run(run):
    if run["completed"] != run["tasks"] or run["completed_db"] != run["tasks"]:
        raise ValueError(f"Incomplete scenario: {run['label']}")
    if len(run["latencies_seconds"]) != run["tasks"]:
        raise ValueError(f"Missing timing rows: {run['label']}")
    if not run["samples"]:
        raise ValueError(f"No monitoring samples: {run['label']}")
    last = run["samples"][-1]
    if last["ready"] != 0 or last["unacked"] != 0:
        raise ValueError(f"Queue not drained at final sample: {run['label']}")
    if last["db_succeeded"] != run["tasks"]:
        raise ValueError(f"DB progress incomplete: {run['label']}")


def run_one(label, limit, workers, tasks, period, run_id, timeout):
    env = {
        **os.environ,
        "RABBIT_MAX_LENGTH": str(limit),
        "ENABLE_BACKPRESSURE": "false" if label == "baseline" else "true",
        "WORKER_PREFETCH_COUNT": (
            "0" if label == "baseline" else os.environ.get("WORKER_CONCURRENCY", "2")
        ),
    }
    prepare_queue(env)
    docker(
        "up",
        "-d",
        "--wait",
        "--scale",
        f"worker={workers}",
        "worker",
        env=env,
        timeout=180,
    )
    wait_for_queue(
        lambda item: item.get("consumers", 0) == workers,
        f"{workers} consumer(s)",
        90,
    )
    input_path = create_input(label, tasks, run_id)
    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "-T",
        "batch",
        "python",
        "-m",
        "issue_intelligence_platform.pipeline.batch",
        f"/app/data/{input_path.name}",
    ]
    events, samples = [], []
    t0 = time.time()
    producer = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def read_events():
        assert producer.stdout is not None
        for line in producer.stdout:
            if line.startswith("METRIC "):
                try:
                    events.append({**json.loads(line[7:]), "seconds": time.time() - t0})
                except ValueError:
                    print(f"[{label}] Malformed METRIC: {line.rstrip()}")
            else:
                print(f"[{label}] {line.rstrip()}")

    reader = threading.Thread(target=read_events, daemon=True)
    reader.start()
    file_env = dotenv_values(ROOT / ".env")
    db_user = (
        os.environ.get("POSTGRES_USER") or file_env.get("POSTGRES_USER") or "issue_app"
    )
    db_name = (
        os.environ.get("POSTGRES_DB")
        or file_env.get("POSTGRES_DB")
        or "issue_intelligence"
    )
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            queue = rabbit()
            if queue is None:
                raise RuntimeError("Work queue disappeared during experiment")
            states = database_states(label, run_id, env, db_user, db_name)
            samples.append(make_sample(t0, queue, states))
            if states.get("FAILED", 0) or states.get("REJECTED", 0):
                raise RuntimeError(f"Task processing failed in {label}: {states}")
            if producer.poll() is not None:
                reader.join(timeout=10)
                if producer.returncode != 0:
                    raise RuntimeError(
                        f"Batch source failed in {label}: {producer.returncode}"
                    )
                confirms = sum(e["event"] == "publish_confirm" for e in events)
                if confirms != tasks:
                    raise RuntimeError(f"Only {confirms}/{tasks} publishes confirmed")
                # A complete database run AND observed empty queue are mandatory.
                if (
                    states.get("SUCCEEDED", 0) == tasks
                    and queue.get("messages_ready", 0) == 0
                    and queue.get("messages_unacknowledged", 0) == 0
                ):
                    break
            time.sleep(period)
        else:
            raise TimeoutError(f"Scenario {label} timed out before verified completion")
    finally:
        if producer.poll() is None:
            producer.terminate()
            try:
                producer.wait(timeout=10)
            except subprocess.TimeoutExpired:
                producer.kill()
        reader.join(timeout=10)
    timings = db_timings(label, run_id, env, db_user, db_name)
    run = {
        "label": label,
        "limit": limit,
        "workers": workers,
        "tasks": tasks,
        "run_id": run_id,
        "duration": time.time() - t0,
        "samples": samples,
        "events": events,
        "latencies_seconds": timings,
        "completed_db": len(timings),
        "completed": sum(x["event"] == "publish_confirm" for x in events),
        "rejected": sum(x["event"] == "publish_rejected" for x in events),
    }
    verify_run(run)
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / f"{label}.json").write_text(
        json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return run


def saturated_rates(run):
    """Calculate worker/source rates in sampled saturated interval; no invented data."""
    samples = run["samples"]
    if run["label"] == "baseline":
        confirms = [
            e["seconds"] for e in run["events"] if e["event"] == "publish_confirm"
        ]
        if len(confirms) < 2:
            return None
        return {
            "type": "producer burst (not steady state)",
            "source": (len(confirms) - 1) / max(confirms[-1] - confirms[0], 0.001),
            "worker": None,
        }
    threshold = 0.8 * run["limit"]
    saturated = [s for s in samples if s["ready"] >= threshold and s["unacked"] > 0]
    if len(saturated) < 3:
        return None
    first, last = saturated[0], saturated[-1]
    interval = last["seconds"] - first["seconds"]
    if interval < 5:
        return None
    confirmed = sum(
        e["event"] == "publish_confirm"
        and first["seconds"] <= e["seconds"] <= last["seconds"]
        for e in run["events"]
    )
    return {
        "type": "saturated sampled interval",
        "source": confirmed / interval,
        "worker": max(0, last["ack_count"] - first["ack_count"]) / interval,
    }


def plot(runs):
    RESULTS.mkdir(parents=True, exist_ok=True)
    for filename, title, column in (
        ("queue_ready.png", "RabbitMQ messages ready", "ready"),
        ("queue_unacked.png", "RabbitMQ unacknowledged", "unacked"),
        ("queue_total.png", "RabbitMQ total = ready + unacknowledged", "total"),
        (
            "db_completed.png",
            "Successfully completed tasks in PostgreSQL",
            "db_succeeded",
        ),
    ):
        plt.figure(figsize=(10, 4))
        for run in runs:
            samples = run["samples"]
            values = [
                (s["ready"] + s["unacked"]) if column == "total" else s[column]
                for s in samples
            ]
            plt.plot([s["seconds"] for s in samples], values, label=run["label"])
        plt.title(title)
        plt.xlabel("Time (seconds)")
        plt.ylabel("Messages / tasks")
        plt.legend()
        plt.tight_layout()
        plt.savefig(RESULTS / filename)
        plt.close()
    for event in ("publish_confirm", "publish_rejected"):
        plt.figure(figsize=(10, 4))
        for run in runs:
            bins = [0] * (int(run["duration"]) + 2)
            for item in run["events"]:
                if item["event"] == event:
                    bins[min(int(item["seconds"]), len(bins) - 1)] += 1
            plt.plot(range(len(bins)), bins, label=run["label"])
        plt.title(f"Source {event} events per second")
        plt.xlabel("Time (seconds)")
        plt.ylabel("Events/s")
        plt.legend()
        plt.tight_layout()
        plt.savefig(RESULTS / f"{event}_rate.png")
        plt.close()
    for index, filename, title in (
        (0, "inference_time.png", "Inference execution time (seconds)"),
        (1, "end_to_end_time.png", "Accepted-to-finished time (seconds)"),
    ):
        plt.figure(figsize=(10, 4))
        values = [[row[index] for row in run["latencies_seconds"]] for run in runs]
        plt.hist(
            values,
            bins=30,
            histtype="step",
            linewidth=1.6,
            label=[run["label"] for run in runs],
        )
        plt.title(title)
        plt.xlabel("Seconds")
        plt.ylabel("Tasks")
        plt.legend()
        plt.tight_layout()
        plt.savefig(RESULTS / filename)
        plt.close()
    plt.figure(figsize=(10, 4))
    for run in runs:
        x, y = [], []
        for sample in run["samples"]:
            x.append(sample["seconds"])
            y.append(sum(sample["memory_mib"].values()))
        plt.plot(x, y, label=run["label"])
    plt.title("Worker + batch Docker memory usage (not process RSS)")
    plt.xlabel("Time (seconds)")
    plt.ylabel("MiB")
    plt.legend()
    plt.tight_layout()
    plt.savefig(RESULTS / "pipeline_memory.png")
    plt.close()


def summarize(runs):
    lines = [
        "**Результаты реального нагрузочного прогона.** Для каждого сценария ",
        "измерения завершаются только после сохранения всех результатов в PostgreSQL ",
        "и наблюдаемого опустошения рабочей очереди.",
        "",
        "| Сценарий | Успешно / отправлено | Ready, макс. | Unacked, макс. | "
        "Подтверждения/с в насыщении | ACK/с в насыщении | Отказов публикации | "
        "Inference, среднее (с) | E2E после приёма, среднее (с) | Время опыта (с) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    rates = []
    for run in runs:
        verify_run(run)
        rate = saturated_rates(run)
        rates.append(rate)
        mean_infer = statistics.mean(p[0] for p in run["latencies_seconds"])
        mean_e2e = statistics.mean(p[1] for p in run["latencies_seconds"])
        peak_ready = max(s["ready"] for s in run["samples"])
        peak_unacked = max(s["unacked"] for s in run["samples"])
        source = f"{rate['source']:.2f}" if rate else "н/д"
        worker = (
            f"{rate['worker']:.2f}" if rate and rate["worker"] is not None else "н/д"
        )
        lines.append(
            f"| {run['label']} | {run['completed_db']}/{run['tasks']} | "
            f"{peak_ready} | {peak_unacked} | {source} | {worker} | "
            f"{run['rejected']} | {mean_infer:.3f} | {mean_e2e:.3f} | "
            f"{run['duration']:.1f} |"
        )
    one, two = rates[1], rates[2]
    if one and two and one["source"] > 0:
        lines.append(
            f"\nОтношение скорости подтверждённой публикации двух воркеров к одному "
            f"на насыщенном участке: **{two['source'] / one['source']:.2f}×**."
        )
    lines.extend(
        [
            "",
            "Показатели подтверждения публикации и ACK рассчитаны на интервале "
            "с наблюдаемой загрузкой очереди не менее 80% её лимита; "
            "если подходящего интервала нет, выводится «н/д». "
            "Для baseline указывается скорость короткого всплеска публикаций, "
            "а не стационарная скорость обработки.",
            "",
            "`x-max-length` ограничивает `ready`, но не `unacked`; "
            "поэтому суммарное количество сообщений может превышать лимит `ready`.",
            "",
            "Latency E2E измерена с момента создания принятой записи в БД, "
            "не включает задержку источника до подтверждения публикации. "
            "Это разные показатели и их нельзя смешивать.",
            "",
            "Память — величина `docker stats` для воркеров и batch-контейнера "
            "только этого Compose-проекта; это не чистый RSS Python-процессов. "
            "Измерения выборочные, короткие всплески могут быть пропущены.",
            "",
        ]
    )
    for filename in (
        "queue_ready.png",
        "queue_unacked.png",
        "queue_total.png",
        "db_completed.png",
        "publish_confirm_rate.png",
        "publish_rejected_rate.png",
        "pipeline_memory.png",
        "inference_time.png",
        "end_to_end_time.png",
    ):
        lines.append(f"![{filename}](results/performance/{filename})")
    lines.append("\nИсходные измерения: `results/performance/*.json`.")
    doc = ROOT / "README.md"
    original = doc.read_text(encoding="utf-8")
    start, end = (
        "<!-- PERFORMANCE_RESULTS_START -->",
        "<!-- PERFORMANCE_RESULTS_END -->",
    )
    if original.count(start) != 1 or original.count(end) != 1:
        raise RuntimeError("README.md has no unique performance-results markers")
    prefix, rest = original.split(start, 1)
    _, suffix = rest.split(end, 1)
    doc.write_text(
        prefix + start + "\n" + "\n".join(lines) + "\n" + end + suffix,
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=240)
    parser.add_argument("--period", type=float, default=1.0)
    parser.add_argument("--bounded-limit", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if (
        args.tasks < 3
        or args.period <= 0
        or args.bounded_limit < 1
        or args.timeout <= 0
    ):
        parser.error("tasks>=3, period>0, bounded-limit>=1, timeout>0 required")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + uuid4().hex[:6]
    specs = (
        ("baseline", max(100000, args.tasks * 2), 1),
        ("bounded_1_worker", args.bounded_limit, 1),
        ("bounded_2_workers", args.bounded_limit, 2),
    )
    runs = []
    for label, limit, worker_count in specs:
        print(f"Starting {label}: queue capacity={limit}, workers={worker_count}")
        runs.append(
            run_one(
                label,
                limit,
                worker_count,
                args.tasks,
                args.period,
                run_id,
                args.timeout,
            )
        )
    plot(runs)
    summarize(runs)
    print("Verified raw data and plots written to", RESULTS)


if __name__ == "__main__":
    main()

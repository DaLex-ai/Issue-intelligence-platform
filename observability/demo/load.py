"""Compare one and two workers on the isolated Compose project."""

import argparse
import json
import os
import re
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from check import alerts, metrics

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "iip-observe-qa"
QA = [
    "docker",
    "compose",
    "-p",
    PROJECT,
    "-f",
    "docker-compose.yml",
    "-f",
    "observability/qa.compose.yaml",
    "--profile",
    "observability",
]
ENV = {**os.environ, "OTEL_SDK_DISABLED": "false"}
OBS = ("alloy", "loki", "tempo", "otel-collector", "prometheus", "grafana")
WORK = 'rabbitmq_detailed_queue_messages_ready{queue="issue.inference.tasks"}'


def run(*args):
    return subprocess.run(
        args, cwd=ROOT, env=ENV, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


def scalar(query):
    values = metrics(query)
    return float(values[0]["value"][1]) if values else None


def memory_snapshot():
    names = [f"{PROJECT}-{service}-1" for service in OBS]
    lines = run("docker", "stats", "--no-stream", "--format", "{{json .}}", *names)
    result = {}
    for line in lines.splitlines():
        entry = json.loads(line)
        match = re.match(r"([0-9.]+)\s*(B|KiB|MiB|GiB)", entry["MemUsage"])
        if not match:
            raise ValueError(entry["MemUsage"])
        amount, unit = match.groups()
        result[entry["Name"]] = (
            float(amount)
            * {
                "B": 1 / 1048576,
                "KiB": 1 / 1024,
                "MiB": 1,
                "GiB": 1024,
            }[unit]
        )
    return result


def succeeded(prefix):
    sql = (
        "SELECT count(*) FROM inference_tasks WHERE state='SUCCEEDED' "
        f"AND summary LIKE '{prefix} %'"
    )
    return int(
        run(
            *QA,
            "exec",
            "-T",
            "db",
            "sh",
            "-c",
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"',
            "sh",
            sql,
        )
    )


def scenario(workers, tasks):
    print(f"Starting {workers} worker(s), {tasks} tasks", flush=True)
    run(
        *QA,
        "up",
        "-d",
        "--no-build",
        "--wait",
        "--scale",
        f"worker={workers}",
        "worker",
    )
    prefix = f"iip-obs-{uuid4().hex[:12]}"
    filename = f"{prefix}.jsonl"
    path = ROOT / "data" / filename
    path.write_text(
        "".join(
            json.dumps({"summary": f"{prefix} server crash {i}"}) + "\n"
            for i in range(tasks)
        ),
        encoding="utf-8",
    )
    snapshots = []
    started = time.monotonic()
    command = [
        *QA,
        "run",
        "--rm",
        "-T",
        "--no-deps",
        "batch",
        "python",
        "-m",
        "issue_intelligence_platform.pipeline.batch",
        f"/app/data/{filename}",
    ]
    try:
        with subprocess.Popen(
            command, cwd=ROOT, env=ENV, stdout=subprocess.DEVNULL
        ) as child:
            deadline = started + max(180, tasks * 2)
            while time.monotonic() < deadline:
                ready = scalar(WORK)
                if ready is None:
                    ready = 0
                row = {
                    "elapsed_s": round(time.monotonic() - started, 2),
                    "ready": ready,
                    "source_per_s": scalar(
                        "sum(rate(iip_source_published_total[30s]))"
                    ),
                    "workers_per_s": scalar(
                        'sum(rate(iip_worker_tasks_total{result="ok"}[30s]))'
                    ),
                    "workers": metrics(
                        'sum by (worker) (rate(iip_worker_tasks_total{result="ok"}[30s]))'
                    ),
                    "alerts": alerts(),
                    "memory_mib": memory_snapshot(),
                }
                snapshots.append(row)
                if child.poll() is not None and child.returncode:
                    raise RuntimeError(f"batch exit code {child.returncode}")
                if (
                    child.poll() is not None
                    and ready == 0
                    and succeeded(prefix) == tasks
                ):
                    break
                if len(snapshots) % 4 == 0:
                    print(
                        {
                            k: row[k]
                            for k in (
                                "elapsed_s",
                                "ready",
                                "source_per_s",
                                "workers_per_s",
                                "alerts",
                            )
                        },
                        flush=True,
                    )
                time.sleep(3)
            else:
                if child.poll() is None:
                    child.terminate()
                raise TimeoutError(f"batch {prefix} did not finish")
    finally:
        path.unlink(missing_ok=True)
    plateau = [
        row
        for row in snapshots
        if row["elapsed_s"] >= 35
        and row["ready"] >= 18
        and row["source_per_s"] is not None
        and row["workers_per_s"] is not None
    ]
    if not plateau:
        raise RuntimeError("no saturated plateau sampled")
    summary = {
        "workers": workers,
        "tasks": tasks,
        "sql_succeeded": succeeded(prefix),
        "ready_peak": max(row["ready"] for row in snapshots),
        "source_per_s_median": statistics.median(
            row["source_per_s"] for row in plateau
        ),
        "workers_per_s_median": statistics.median(
            row["workers_per_s"] for row in plateau
        ),
        "observability_peak_mib": round(
            max(sum(row["memory_mib"].values()) for row in snapshots), 1
        ),
        "queue_alert_fired": any(
            row["alerts"].get("Work queue above 90 percent") == "firing"
            for row in snapshots
        ),
        "dlq_alert_fired": any(
            row["alerts"].get("Dead letter queue has messages") == "firing"
            for row in snapshots
        ),
        "samples": snapshots,
    }
    print(json.dumps({k: v for k, v in summary.items() if k != "samples"}), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=420)
    parser.add_argument(
        "--workers", type=int, choices=[1, 2], nargs="+", default=[1, 2]
    )
    args = parser.parse_args()
    if not 360 <= args.tasks <= 6000:
        parser.error("--tasks must be between 360 and 6000")
    output = ROOT / "results" / "observability"
    output.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = output / f"load-{now}.json"
    results = []
    for workers in args.workers:
        results.append(scenario(workers, args.tasks))
        report.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()

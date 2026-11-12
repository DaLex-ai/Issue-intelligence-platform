"""Fill the disposable queue, verify HTTP 503 traces and both firing alerts."""

import json
import time
from datetime import UTC, datetime
from urllib.error import HTTPError

from check import LOKI, alerts, final_state, get, retry, task, trace_spans
from load import PROJECT, QA, ROOT, WORK, run, scalar


def overload_trace(since):
    response = get(
        LOKI,
        "/loki/api/v1/query_range",
        params={
            "query": '{service="app"} | json | status_code="503"',
            "start": since,
            "limit": 100,
        },
    )
    for stream in response["data"]["result"]:
        for _, line in stream["values"]:
            event = json.loads(line)
            spans = trace_spans(event["trace_id"])
            for _, span in spans:
                if span["name"] == "POST /api/v1/tasks" and span.get("status", {}).get(
                    "code"
                ) in {2, "STATUS_CODE_ERROR"}:
                    return event["trace_id"]
    return None


def main():
    count = len(
        run(
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--filter",
            "label=com.docker.compose.service=worker",
            "--format",
            "{{.ID}}",
        ).splitlines()
    )
    assert count > 0, "start QA workers first"
    limit = int(scalar("max(iip_queue_limit)"))
    ids = []
    refused = 0
    since = time.time_ns()
    try:
        run(*QA, "stop", "worker")
        for i in range(limit + 8):
            try:
                ids.append(task(f"observability overload {i}"))
            except HTTPError as exc:
                assert exc.code == 503, exc.code
                refused += 1
        assert len(ids) == limit and refused == 8, (len(ids), refused)
        retry(lambda: scalar(WORK) == limit)
        trace_id = retry(lambda: overload_trace(since))
        print(
            json.dumps(
                {"accepted": len(ids), "http_503": refused, "trace_503": trace_id}
            ),
            flush=True,
        )
        fired = retry(
            lambda: (
                all(value == "firing" for value in alerts().values())
                and len(alerts()) == 2
            ),
            timeout=100,
        )
        assert fired
    finally:
        run(
            *QA,
            "up",
            "-d",
            "--no-build",
            "--wait",
            "--scale",
            f"worker={count}",
            "worker",
        )
    retry(
        lambda: all(final_state(task_id) == "SUCCEEDED" for task_id in ids), timeout=90
    )
    result = {
        "accepted": len(ids),
        "http_503": refused,
        "trace_503": trace_id,
        "queue_alert": "firing",
        "dlq_alert": "firing",
        "succeeded_after_resume": len(ids),
    }
    output = ROOT / "results" / "observability"
    output.mkdir(parents=True, exist_ok=True)
    filename = datetime.now(UTC).strftime("burst-%Y%m%dT%H%M%SZ.json")
    (output / filename).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

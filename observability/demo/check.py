"""Smoke checks for the isolated Compose project's published QA ports."""

import argparse
import base64
import json
import os
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "http://127.0.0.1:18080"
LOKI = "http://127.0.0.1:13100"
TEMPO = "http://127.0.0.1:13200"
PROM = "http://127.0.0.1:19090"
GRAFANA = "http://127.0.0.1:13000"


def get(base, path, *, params=None, payload=None, auth=None):
    url = base + path + ("?" + urlencode(params) if params else "")
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    request = Request(
        url, data=json.dumps(payload).encode() if payload else None, headers=headers
    )
    with urlopen(request, timeout=8) as response:
        return json.load(response)


def retry(check, timeout=45):
    deadline = time.monotonic() + timeout
    error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (HTTPError, OSError, ValueError, AssertionError) as exc:
            error = exc
        time.sleep(1)
    raise RuntimeError(f"check timed out: {error}")


def task(summary="observability smoke"):
    return get(API, "/api/v1/tasks", payload={"summary": summary})["task_id"]


def final_state(task_id):
    value = get(API, "/api/v1/tasks/" + task_id)
    state = value.get("status") or value.get("state")
    return state if state in {"SUCCEEDED", "FAILED"} else None


def logs(task_id):
    response = get(
        LOKI,
        "/loki/api/v1/query_range",
        params={
            "query": '{service=~".+"} | json | task_id="' + task_id + '"',
            "start": time.time_ns() - 900 * 10**9,
            "limit": 500,
        },
    )
    streams = response["data"]["result"]
    return [json.loads(line) for stream in streams for _, line in stream["values"]]


def trace_spans(trace_id):
    trace = get(TEMPO, "/api/traces/" + trace_id)
    result = []
    for batch in trace.get("batches", trace.get("resourceSpans", [])):
        resource = {
            attr["key"]: next(iter(attr["value"].values()))
            for attr in batch["resource"]["attributes"]
        }
        for scope in batch.get(
            "scopeSpans", batch.get("instrumentationLibrarySpans", [])
        ):
            result.extend((resource, span) for span in scope["spans"])
    return result


def metrics(query):
    response = get(PROM, "/api/v1/query", params={"query": query})
    return response["data"]["result"]


def alerts():
    auth = "admin:" + os.getenv("GRAFANA_ADMIN_PASSWORD", "local-observe")
    value = get(GRAFANA, "/api/prometheus/grafana/api/v1/rules", auth=auth)
    return {
        rule["name"]: rule.get("state")
        for group in value["data"]["groups"]
        for rule in group["rules"]
    }


def check_task(poison=False):
    task_id = task("iip-demo-poison" if poison else "observability smoke")
    state = retry(lambda: final_state(task_id))
    assert state == ("FAILED" if poison else "SUCCEEDED"), (task_id, state)

    def correlated():
        events = logs(task_id)
        records = [
            event
            for event in events
            if event["message"]
            in {
                "task_sent_to_dlq",
                "task_completed",
            }
        ]
        if not records:
            return None
        trace_id = records[0]["trace_id"]
        spans = trace_spans(trace_id)
        names = {span["name"] for _, span in spans}
        if "POST /api/v1/tasks" not in names or "inference" not in names:
            return None
        assert all(
            event["trace_id"] == trace_id for event in events if "trace_id" in event
        )
        process_ids = {
            span["spanId"] for _, span in spans if span["name"] == "rabbitmq process"
        }
        for _, span in spans:
            if span["name"] == "inference":
                assert span["parentSpanId"] in process_ids
        if not poison:
            assert "postgresql write result" in names
        else:
            if len(process_ids) < 3:
                return None
            assert any(event["message"] == "task_sent_to_dlq" for event in events)
        durations = {
            span["name"]: round(
                (int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])) / 1e6, 2
            )
            for _, span in spans
            if span["name"] == "inference"
        }
        queue_wait = [
            float(attr["value"]["doubleValue"])
            for _, span in spans
            for attr in span.get("attributes", [])
            if attr["key"] == "messaging.queue_wait_seconds"
        ]
        return {
            "task_id": task_id,
            "trace_id": trace_id,
            "state": state,
            "span_names": sorted(names),
            "process_attempts": len(process_ids),
            "log_services": sorted({event["service"] for event in events}),
            "queue_wait_seconds": queue_wait,
            "durations_ms": durations,
        }

    return retry(correlated, timeout=90)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", choices=["task", "poison", "alerts", "metrics", "basic"]
    )
    parser.add_argument("query", nargs="?")
    args = parser.parse_args()
    if args.mode == "alerts":
        result = alerts()
    elif args.mode == "metrics":
        result = metrics(args.query)
    elif args.mode == "basic":
        task_id = task()
        result = {"task_id": task_id, "state": retry(lambda: final_state(task_id))}
        assert result["state"] == "SUCCEEDED"
    else:
        result = check_task(args.mode == "poison")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

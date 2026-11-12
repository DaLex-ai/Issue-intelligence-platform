import json
import logging
import os
import re
import sys
import traceback
from datetime import UTC, datetime

from opentelemetry import trace

from issue_intelligence_platform.core.telemetry import service_name, task_id_context

_DSN = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s@]+@")
_SECRET = re.compile(
    r"(?i)(password|passwd|secret|token|authorization)([=:]\s*)[^\s,;]+"
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Library messages can embed raw SQL/AMQP payloads. Keep their template
        # and stack frames, but do not interpolate arbitrary library arguments.
        project = record.name.startswith(("issue_intelligence", "__main__"))
        message = record.getMessage() if project else str(record.msg)
        if not project and not record.args:
            message = "library_event"
        message = _SECRET.sub(r"\1\2[redacted]", _DSN.sub(r"\1[redacted]@", message))
        event = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": service_name(),
            "env": os.getenv("IIP_ENV", "local"),
            "logger": record.name,
            "message": message,
        }
        task_id = task_id_context.get() or getattr(record, "task_id", None)
        if task_id:
            event["task_id"] = task_id
        context = trace.get_current_span().get_span_context()
        if context.is_valid:
            event["trace_id"] = f"{context.trace_id:032x}"
            event["span_id"] = f"{context.span_id:016x}"
        for field in (
            "method",
            "path",
            "status_code",
            "duration_ms",
            "component",
            "error_type",
            "event",
            "count",
            "attempt",
            "delay_seconds",
            "model_alias",
            "model_version",
        ):
            if hasattr(record, field):
                event[field] = getattr(record, field)
        if record.exc_info:
            exc_type, _, stack = record.exc_info
            event["exception"] = "".join(
                traceback.format_list(traceback.extract_tb(stack))
            )
            event["error_type"] = exc_type.__name__ if exc_type else "Unknown"
        return json.dumps(event, ensure_ascii=False)


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.captureWarnings(True)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
    logging.getLogger("uvicorn.access").disabled = True

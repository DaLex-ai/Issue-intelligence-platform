"""Optional OTLP telemetry shared by the API, publisher and worker."""

import os
import socket
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache

from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import StatusCode

from issue_intelligence_platform import __version__

task_id_context: ContextVar[str | None] = ContextVar("task_id", default=None)
_service = "app"
_providers = []


def setup(service: str) -> None:
    global _service
    _service = os.getenv("OTEL_SERVICE_NAME", service)
    if _providers or os.getenv("OTEL_SDK_DISABLED", "true").lower() == "true":
        return

    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    attributes = dict(Resource.create().attributes)
    attributes["service.name"] = _service
    attributes.setdefault(
        "service.version", os.getenv("IIP_SERVICE_VERSION", __version__)
    )
    attributes.setdefault("deployment.environment.name", os.getenv("IIP_ENV", "local"))
    if "service.instance.id=" not in os.getenv("OTEL_RESOURCE_ATTRIBUTES", ""):
        attributes["service.instance.id"] = os.getenv(
            "IIP_INSTANCE_ID", socket.gethostname()
        )
    resource = Resource(attributes)
    if os.getenv("OTEL_TRACES_EXPORTER", "otlp") != "none":
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(timeout=1)))
        trace.set_tracer_provider(provider)
        _providers.append(provider)

    if os.getenv("OTEL_METRICS_EXPORTER", "otlp") != "none":
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.metrics.view import (
            ExplicitBucketHistogramAggregation,
            View,
        )

        from issue_intelligence_platform.core.config import settings

        delay = max(settings.inference_delay_seconds, 0.01)
        provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(timeout=1),
                    export_interval_millis=1000,
                )
            ],
            views=[
                View(
                    instrument_name="iip_task_duration_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(
                        [delay * x for x in (1, 2, 4, 8, 16, 32, 64, 128)]
                    ),
                ),
                View(
                    instrument_name="iip_inference_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(
                        [
                            delay * x
                            for x in (0.25, 0.5, 0.75, 1, 1.05, 1.15, 1.5, 2, 4, 8)
                        ]
                    ),
                ),
                View(
                    instrument_name="iip_api_duration_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(
                        [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10]
                    ),
                ),
            ],
        )
        meter = provider.get_meter(__name__)
        meter.create_observable_gauge(
            "iip_process_resident_memory_bytes",
            callbacks=[_rss],
        )
        if service == "app" and settings.enable_backpressure:
            meter.create_observable_gauge(
                "iip_queue_limit",
                callbacks=[lambda _: [metrics.Observation(settings.rabbit_max_length)]],
            )
        metrics.set_meter_provider(provider)
        _providers.append(provider)


def _rss(_options):
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            pages = int(handle.read().split()[1])
        return [metrics.Observation(pages * os.sysconf("SC_PAGE_SIZE"))]
    except (OSError, ValueError, IndexError):
        return []


def flush() -> None:
    for provider in _providers:
        provider.force_flush(timeout_millis=1500)


def shutdown() -> None:
    for provider in _providers:
        provider.shutdown()


def service_name() -> str:
    return _service


@contextmanager
def task_scope(task_id):
    token = task_id_context.set(str(task_id) if task_id else None)
    try:
        yield
    finally:
        task_id_context.reset(token)


def span(name, **kwargs):
    return trace.get_tracer(__name__).start_as_current_span(
        name, record_exception=False, set_status_on_exception=False, **kwargs
    )


def fail(error):
    current = trace.get_current_span()
    error_type = (
        type(error).__name__ if isinstance(error, BaseException) else str(error)
    )
    current.set_status(StatusCode.ERROR, error_type)
    current.record_exception(RuntimeError(error_type))


def outgoing_headers() -> dict:
    headers = {}
    inject(headers)
    return headers


def parent_context(headers):
    return extract(
        {
            key: value.decode("ascii", errors="replace")
            if isinstance(value, bytes)
            else value
            for key, value in (headers or {}).items()
            if key in {"traceparent", "tracestate"} and isinstance(value, (str, bytes))
        }
    )


@lru_cache
def instrument(name, kind):
    meter = metrics.get_meter(__name__)
    return getattr(meter, f"create_{kind}")(name)


def count(name, **labels):
    instrument(name, "counter").add(1, labels)


def observe(name, value, **labels):
    instrument(name, "histogram").record(value, labels)


def active(delta):
    instrument("iip_worker_in_flight", "up_down_counter").add(delta)


def wait_seconds(value):
    instrument("iip_source_wait_seconds", "counter").add(value)

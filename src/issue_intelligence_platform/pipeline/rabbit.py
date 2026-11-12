"""RabbitMQ topology and a publisher with broker-confirmed admission.

The FastStream worker consumes the same declared queue; aio-pika is used for
publish confirms because nack on x-overflow must be surfaced to API/batch.
"""

import asyncio
import json
import logging
import time
from uuid import UUID

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.exceptions import AMQPException, DeliveryError
from faststream.rabbit import RabbitQueue
from opentelemetry.trace import SpanKind

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.core.telemetry import (
    fail,
    outgoing_headers,
    span,
    task_scope,
)

logger = logging.getLogger(__name__)


class QueueFull(Exception):
    """The broker explicitly refused responsibility for this publication."""


class PublishUncertain(Exception):
    """A timeout/disconnection: the message MAY have reached the broker."""


def queue_arguments() -> dict:
    arguments = {
        "x-dead-letter-exchange": settings.rabbit_dlx,
        "x-dead-letter-routing-key": settings.rabbit_dlq_routing_key,
    }
    if settings.enable_backpressure:
        arguments["x-max-length"] = settings.rabbit_max_length
        arguments["x-overflow"] = "reject-publish"
    return arguments


def work_queue() -> RabbitQueue:
    return RabbitQueue(settings.rabbit_queue, durable=True, arguments=queue_arguments())


async def declare_topology(channel) -> None:
    dlx = await channel.declare_exchange(
        settings.rabbit_dlx, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlq = await channel.declare_queue(settings.rabbit_dlq, durable=True)
    await dlq.bind(dlx, routing_key=settings.rabbit_dlq_routing_key)
    await channel.declare_queue(
        settings.rabbit_queue,
        durable=True,
        arguments=queue_arguments(),
    )


class ConfirmingPublisher:
    def __init__(self):
        self.connection = None
        self.channel = None

    def ready(self) -> bool:
        return bool(
            self.connection is not None
            and self.connection.connected.is_set()
            and self.channel is not None
            and not self.channel.is_closed
        )

    async def start(self):
        try:
            self.connection = await aio_pika.connect_robust(settings.rabbit_url)
            self.channel = await self.connection.channel(
                publisher_confirms=True,
                on_return_raises=True,
            )
            await declare_topology(self.channel)
        except BaseException:
            await self.close()
            raise

    async def close(self):
        if self.connection is not None:
            await self.connection.close()

    async def publish(self, task_id: UUID, summary: str, description: str) -> None:
        with (
            task_scope(task_id),
            span("rabbitmq publish", kind=SpanKind.PRODUCER) as current,
        ):
            current.set_attribute("messaging.system", "rabbitmq")
            current.set_attribute("messaging.destination.name", settings.rabbit_queue)
            current.set_attribute("task.id", str(task_id))
            try:
                await self._publish(task_id, summary, description)
            except (QueueFull, PublishUncertain) as exc:
                fail(exc)
                if isinstance(exc, QueueFull):
                    logger.warning("broker_rejected_publish")
                raise

    async def _publish(self, task_id: UUID, summary: str, description: str) -> None:
        if self.channel is None:
            raise PublishUncertain("publisher not connected")
        payload = {
            "task_id": str(task_id),
            "summary": summary,
            "description": description,
        }
        message = Message(
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(task_id),
            correlation_id=str(task_id),
            headers={**outgoing_headers(), "iip-enqueued-ns": str(time.time_ns())},
        )
        try:
            confirmation = await asyncio.wait_for(
                self.channel.default_exchange.publish(
                    message,
                    routing_key=settings.rabbit_queue,
                    mandatory=True,
                    timeout=settings.publisher_confirm_timeout,
                ),
                timeout=settings.publisher_confirm_timeout,
            )
        except DeliveryError as exc:
            raise QueueFull(
                "broker rejected publish (queue full or unroutable)"
            ) from exc
        except (AMQPException, OSError, TimeoutError, RuntimeError) as exc:
            raise PublishUncertain("publish outcome is unknown") from exc
        if confirmation is False:
            raise QueueFull("negative publisher confirm")

"""RabbitMQ topology and a publisher with broker-confirmed admission.

The FastStream worker consumes the same declared queue; aio-pika is used for
publish confirms because nack on x-overflow must be surfaced to API/batch.
"""

import json
from uuid import UUID

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitQueue

from issue_intelligence_platform.core.config import settings


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

    async def start(self):
        self.connection = await aio_pika.connect_robust(settings.rabbit_url)
        self.channel = await self.connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        await declare_topology(self.channel)

    async def close(self):
        if self.connection is not None:
            await self.connection.close()

    async def publish(self, task_id: UUID, summary: str, description: str) -> None:
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
        )
        try:
            confirmation = await self.channel.default_exchange.publish(
                message,
                routing_key=settings.rabbit_queue,
                mandatory=True,
                timeout=settings.publisher_confirm_timeout,
            )
        except DeliveryError as exc:
            raise QueueFull(
                "broker rejected publish (queue full or unroutable)"
            ) from exc
        except (OSError, TimeoutError, ConnectionError, RuntimeError) as exc:
            raise PublishUncertain("publish outcome is unknown") from exc
        if confirmation is False:
            raise QueueFull("negative publisher confirm")

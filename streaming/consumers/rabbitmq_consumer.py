from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence

import aio_pika
from pydantic import ValidationError

from common.config import StreamSettings
from streaming.consumers.base import EventConsumer, ReceivedEvent
from streaming.events import SaleEvent

logger = logging.getLogger(__name__)


class RabbitMqEventConsumer(EventConsumer):
    def __init__(self, settings: StreamSettings, prefetch: int = 500) -> None:
        self._settings = settings
        self._prefetch = prefetch
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None

    async def start(self) -> None:
        self._connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch)
        exchange = await channel.declare_exchange(
            self._settings.rabbitmq_exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )
        self._queue = await channel.declare_queue(self._settings.rabbitmq_queue, durable=True)
        await self._queue.bind(exchange, routing_key="sales.#")

    async def stop(self) -> None:
        if self._connection is not None:
            await self._connection.close()

    async def events(self) -> AsyncIterator[ReceivedEvent]:
        assert self._queue is not None
        async with self._queue.iterator() as messages:
            async for message in messages:
                try:
                    event = SaleEvent.from_bytes(message.body)
                except ValidationError as exc:
                    logger.warning("invalid_event message_id=%s error=%s", message.message_id, exc)
                    # Sin requeue: va a la dead-letter si la cola tiene una configurada
                    await message.reject(requeue=False)
                    continue
                yield ReceivedEvent(event=event, ack_handle=message)

    async def commit(self, pending: Sequence[ReceivedEvent]) -> None:
        if not pending:
            return
        # Un único ack con multiple=True confirma todas las entregas previas del canal
        last: aio_pika.abc.AbstractIncomingMessage = pending[-1].ack_handle
        await last.ack(multiple=True)

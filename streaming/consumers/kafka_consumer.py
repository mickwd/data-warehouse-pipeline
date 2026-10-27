from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from common.config import StreamSettings
from streaming.consumers.base import EventConsumer, ReceivedEvent
from streaming.events import SaleEvent

logger = logging.getLogger(__name__)


class KafkaEventConsumer(EventConsumer):
    def __init__(self, settings: StreamSettings) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def events(self) -> AsyncIterator[ReceivedEvent]:
        async for message in self._consumer:
            try:
                event = SaleEvent.from_bytes(message.value)
            except ValidationError as exc:
                # Mensaje inválido: se loguea y se sigue, no se bloquea la partición
                logger.warning("invalid_event partition=%s offset=%s error=%s", message.partition, message.offset, exc)
                continue
            yield ReceivedEvent(event=event, ack_handle=message)

    async def commit(self, pending: Sequence[ReceivedEvent]) -> None:
        # Kafka confirma por offset: alcanza con commitear la posición actual del grupo
        if pending:
            await self._consumer.commit()

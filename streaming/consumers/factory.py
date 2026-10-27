from __future__ import annotations

from common.config import StreamSettings
from streaming.consumers.base import EventConsumer


def build_consumer(settings: StreamSettings) -> EventConsumer:
    """Crea el consumidor según `STREAM_BROKER`.

    Los imports son diferidos para no exigir las dos librerías de cliente
    cuando solo se usa un broker.
    """
    if settings.broker == "kafka":
        from streaming.consumers.kafka_consumer import KafkaEventConsumer

        return KafkaEventConsumer(settings)
    if settings.broker == "rabbitmq":
        from streaming.consumers.rabbitmq_consumer import RabbitMqEventConsumer

        return RabbitMqEventConsumer(settings)
    raise ValueError(f"Broker no soportado: {settings.broker}")

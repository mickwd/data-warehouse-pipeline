"""Producer de eventos simulados hacia Kafka o RabbitMQ.

Uso:
    python -m streaming.producer                 # broker según STREAM_BROKER
    python -m streaming.producer --broker rabbitmq --rate 50
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Protocol

from common.config import StreamSettings, get_settings
from common.logs import configure_logging
from streaming.events import SaleEvent
from streaming.simulator import SalesEventSimulator

logger = logging.getLogger("streaming.producer")


class EventPublisher(Protocol):
    async def start(self) -> None: ...
    async def publish(self, event: SaleEvent) -> None: ...
    async def stop(self) -> None: ...


class KafkaEventPublisher:
    def __init__(self, settings: StreamSettings) -> None:
        self._settings = settings
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            linger_ms=20,
            compression_type="gzip",
        )
        await self._producer.start()

    async def publish(self, event: SaleEvent) -> None:
        assert self._producer is not None
        # Particionar por canal mantiene el orden relativo dentro de cada canal
        await self._producer.send(self._settings.kafka_topic, value=event.to_bytes(), key=event.channel.encode())

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()


class RabbitMqEventPublisher:
    def __init__(self, settings: StreamSettings) -> None:
        self._settings = settings
        self._connection = None
        self._exchange = None

    async def start(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await channel.declare_exchange(
            self._settings.rabbitmq_exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )

    async def publish(self, event: SaleEvent) -> None:
        import aio_pika

        assert self._exchange is not None
        message = aio_pika.Message(
            body=event.to_bytes(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=event.event_id,
        )
        await self._exchange.publish(message, routing_key=f"sales.{event.channel}")

    async def stop(self) -> None:
        if self._connection is not None:
            await self._connection.close()


def build_publisher(settings: StreamSettings, broker: str | None = None) -> EventPublisher:
    match broker or settings.broker:
        case "kafka":
            return KafkaEventPublisher(settings)
        case "rabbitmq":
            return RabbitMqEventPublisher(settings)
        case other:
            raise ValueError(f"Broker no soportado: {other}")


async def run_producer(broker: str | None, rate: float, seed: int | None) -> None:
    settings = get_settings().stream
    publisher = build_publisher(settings, broker)
    simulator = SalesEventSimulator(seed=seed)
    interval = 1.0 / rate

    await publisher.start()
    logger.info("producer_started broker=%s rate=%s", broker or settings.broker, rate)
    sent = 0
    try:
        while True:
            await publisher.publish(simulator.next_event())
            sent += 1
            if sent % 500 == 0:
                logger.info("producer_progress sent=%s", sent)
            await asyncio.sleep(interval)
    finally:
        await publisher.stop()


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Simulador de eventos de venta")
    parser.add_argument("--broker", choices=["kafka", "rabbitmq"], default=None)
    parser.add_argument("--rate", type=float, default=settings.stream.producer_events_per_second)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    configure_logging(settings.log_level)
    try:
        asyncio.run(run_producer(args.broker, args.rate, args.seed))
    except KeyboardInterrupt:
        logger.info("producer_stopped")


if __name__ == "__main__":
    main()

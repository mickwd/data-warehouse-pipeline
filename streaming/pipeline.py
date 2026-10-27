"""Consumidor asyncio: eventos -> ventanas -> Redis Pub/Sub + PostgreSQL.

Uso:
    python -m streaming.pipeline

Hay dos tareas concurrentes:
- `_consume`: lee del broker y alimenta los agregadores (CPU mínima por evento).
- `_ticker`: cada `tick_seconds` cierra ventanas vencidas aunque no lleguen
  eventos, publica, persiste y recién ahí confirma los offsets/acks.

Ambas comparten estado en memoria y corren en el mismo event loop, por lo que
no hace falta locking entre ellas salvo para el flush (se usa un asyncio.Lock).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

from common.config import Settings, get_settings
from common.logs import configure_logging
from streaming.consumers.base import EventConsumer, ReceivedEvent
from streaming.windows import WindowAggregator, WindowResult

logger = logging.getLogger("streaming.pipeline")


class LivePublisher(Protocol):
    """Destino en vivo de las ventanas cerradas (Redis Pub/Sub)."""

    async def publish(self, results: Sequence[WindowResult]) -> None: ...


class WindowRepository(Protocol):
    """Persistencia de históricos (PostgreSQL)."""

    async def upsert(self, results: Sequence[WindowResult]) -> None: ...


class MetricsPipeline:
    def __init__(
        self,
        consumer: EventConsumer,
        aggregators: Sequence[WindowAggregator],
        live_publisher: LivePublisher,
        repository: WindowRepository,
        tick_seconds: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._aggregators = list(aggregators)
        self._live = live_publisher
        self._repo = repository
        self._tick_seconds = tick_seconds
        self._pending: list[ReceivedEvent] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        async with self._consumer:
            consume = asyncio.create_task(self._consume(), name="consume")
            ticker = asyncio.create_task(self._ticker(), name="ticker")
            await self._stop.wait()
            consume.cancel()
            ticker.cancel()
            await asyncio.gather(consume, ticker, return_exceptions=True)
            # Al apagar se emiten las ventanas abiertas para no perderlas
            await self._flush(final=True)

    async def _consume(self) -> None:
        try:
            async for received in self._consumer.events():
                event = received.event
                for agg in self._aggregators:
                    agg.add(event.event_ts, event.amount, group=event.channel)
                self._pending.append(received)
        finally:
            # Si el stream termina (o se cae el consumidor) se apaga el pipeline ordenadamente
            self.stop()

    async def _ticker(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._tick_seconds)
            try:
                await self._flush()
            except Exception:
                # Si falla Redis/Postgres no se confirman los eventos, el broker los reentrega
                logger.exception("flush_failed pending=%s", len(self._pending))

    async def _flush(self, final: bool = False) -> None:
        async with self._lock:
            now = datetime.now(UTC)
            results: list[WindowResult] = []
            for agg in self._aggregators:
                results.extend(agg.flush() if final else agg.close_expired(now))

            if results:
                # Primero se persiste: si Redis falla, el histórico igual queda completo
                await self._repo.upsert(results)
                await self._live.publish(results)
                logger.info(
                    "windows_emitted count=%s late=%s",
                    len(results),
                    {a.window_type: a.late_events for a in self._aggregators},
                )

            if self._pending:
                to_commit, self._pending = self._pending, []
                await self._consumer.commit(to_commit)


def build_aggregators(settings: Settings) -> list[WindowAggregator]:
    s = settings.stream
    lateness = timedelta(seconds=s.allowed_lateness_seconds)
    return [
        WindowAggregator(size=timedelta(seconds=s.tumbling_window_seconds), allowed_lateness=lateness),
        WindowAggregator(
            size=timedelta(seconds=s.sliding_window_seconds),
            step=timedelta(seconds=s.sliding_step_seconds),
            allowed_lateness=lateness,
        ),
    ]


async def main_async() -> None:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import create_async_engine

    from streaming.consumers.factory import build_consumer
    from streaming.publisher import RedisMetricsPublisher
    from streaming.storage import WindowMetricsRepository

    settings = get_settings()
    configure_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    engine = create_async_engine(settings.metrics_db.url, pool_pre_ping=True)
    repo = WindowMetricsRepository(engine)
    await repo.create_schema()

    pipeline = MetricsPipeline(
        consumer=build_consumer(settings.stream),
        aggregators=build_aggregators(settings),
        live_publisher=RedisMetricsPublisher(redis, settings.redis.metrics_channel),
        repository=repo,
        tick_seconds=settings.stream.tick_seconds,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Windows no soporta add_signal_handler, ahí corta Ctrl+C
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, pipeline.stop)

    logger.info("pipeline_started broker=%s", settings.stream.broker)
    try:
        await pipeline.run()
    finally:
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("pipeline_stopped")


if __name__ == "__main__":
    main()

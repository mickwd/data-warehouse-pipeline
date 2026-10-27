"""Tests del simulador, del esquema de eventos y del pipeline con dobles en memoria."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

import pytest

from streaming.consumers.base import EventConsumer, ReceivedEvent
from streaming.events import SaleEvent
from streaming.pipeline import MetricsPipeline
from streaming.simulator import SalesEventSimulator
from streaming.windows import WindowAggregator, WindowResult

T0 = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)


def test_simulator_is_deterministic_with_seed() -> None:
    a = [e.model_dump() for e in SalesEventSimulator(seed=7).stream(T0, 10, 50)]
    b = [e.model_dump() for e in SalesEventSimulator(seed=7).stream(T0, 10, 50)]
    assert a == b


def test_simulator_produces_valid_events() -> None:
    events = list(SalesEventSimulator(seed=1, late_event_ratio=0.0).stream(T0, 10, 200))
    assert all(e.amount > 0 and e.quantity >= 1 for e in events)
    assert {e.channel for e in events} <= {"web", "mobile", "store", "marketplace"}
    assert events[-1].event_ts == T0 + timedelta(seconds=19.9)
    assert len({e.event_id for e in events}) == 200


def test_event_roundtrip_and_utc_normalization() -> None:
    event = SaleEvent(
        event_id="e1",
        event_ts=datetime(2024, 5, 1, 12, 0),
        store_id=1,
        product_id="P00001",
        channel="web",
        quantity=1,
        amount=10.5,
    )
    assert event.event_ts.tzinfo == UTC
    assert SaleEvent.from_bytes(event.to_bytes()) == event


class FakeConsumer(EventConsumer):
    def __init__(self, events: Sequence[SaleEvent]) -> None:
        self._events = events
        self.committed: list[ReceivedEvent] = []

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def events(self) -> AsyncIterator[ReceivedEvent]:
        for event in self._events:
            yield ReceivedEvent(event=event)
            await asyncio.sleep(0)

    async def commit(self, pending: Sequence[ReceivedEvent]) -> None:
        self.committed.extend(pending)


class Recorder:
    def __init__(self) -> None:
        self.results: list[WindowResult] = []

    async def publish(self, results: Sequence[WindowResult]) -> None:
        self.results.extend(results)

    async def upsert(self, results: Sequence[WindowResult]) -> None:
        self.results.extend(results)


def test_pipeline_persists_publishes_and_commits_everything() -> None:
    events = list(SalesEventSimulator(seed=3, late_event_ratio=0.0).stream(T0, 20, 400))
    consumer = FakeConsumer(events)
    live, repo = Recorder(), Recorder()
    pipeline = MetricsPipeline(
        consumer=consumer,
        aggregators=[WindowAggregator(size=timedelta(seconds=5))],
        live_publisher=live,
        repository=repo,
        tick_seconds=0.01,
    )

    asyncio.run(asyncio.wait_for(pipeline.run(), timeout=10))

    totals = [r for r in repo.results if r.group_key == "all"]
    assert sum(r.stats.count for r in totals) == len(events)
    # cada ventana redondea a 2 decimales, se tolera esa diferencia acumulada
    assert sum(r.stats.total for r in totals) == pytest.approx(sum(e.amount for e in events), abs=0.05)
    assert live.results == repo.results
    assert len(consumer.committed) == len(events)

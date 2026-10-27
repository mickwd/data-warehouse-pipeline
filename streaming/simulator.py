"""Generador de eventos de venta sintéticos.

Lógica pura y determinística con semilla, separada del producer para poder
testearla y reutilizarla con cualquier broker.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import numpy as np

from streaming.events import Channel, SaleEvent

CHANNELS: tuple[Channel, ...] = ("web", "mobile", "store", "marketplace")
CHANNEL_WEIGHTS = np.array([0.45, 0.30, 0.15, 0.10])
# Ticket medio por canal (lognormal: muchas compras chicas y alguna grande)
CHANNEL_MEAN_LOG_AMOUNT = {"web": 9.2, "mobile": 9.0, "store": 9.5, "marketplace": 8.8}


class SalesEventSimulator:
    def __init__(
        self,
        seed: int | None = None,
        n_products: int = 500,
        n_stores: int = 25,
        late_event_ratio: float = 0.02,
        max_lateness: timedelta = timedelta(seconds=20),
    ) -> None:
        self._rng = np.random.default_rng(seed)
        self._n_products = n_products
        self._n_stores = n_stores
        self._late_ratio = late_event_ratio
        self._max_lateness = max_lateness

    def next_event(self, now: datetime | None = None) -> SaleEvent:
        now = now or datetime.now(UTC)
        channel: Channel = CHANNELS[int(self._rng.choice(len(CHANNELS), p=CHANNEL_WEIGHTS))]
        quantity = int(self._rng.integers(1, 6))
        unit_amount = float(self._rng.lognormal(CHANNEL_MEAN_LOG_AMOUNT[channel], 0.6))

        event_ts = now
        # Un porcentaje chico llega desordenado, como pasa con clientes móviles offline
        if self._rng.random() < self._late_ratio:
            event_ts = now - timedelta(seconds=float(self._rng.uniform(1, self._max_lateness.total_seconds())))

        return SaleEvent(
            event_id=str(uuid.UUID(bytes=self._rng.bytes(16), version=4)),
            event_ts=event_ts,
            store_id=int(self._rng.integers(1, self._n_stores + 1)),
            product_id=f"P{int(self._rng.integers(1, self._n_products + 1)):05d}",
            channel=channel,
            quantity=quantity,
            amount=round(unit_amount * quantity, 2),
        )

    def stream(self, start: datetime, events_per_second: float, count: int) -> Iterator[SaleEvent]:
        """Secuencia con event time sintético, útil para pruebas y backfills."""
        interval = timedelta(seconds=1 / events_per_second)
        for i in range(count):
            yield self.next_event(start + i * interval)

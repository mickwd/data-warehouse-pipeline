"""Interfaz común de consumo, independiente del broker."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any

from streaming.events import SaleEvent


@dataclass(frozen=True, slots=True)
class ReceivedEvent:
    """Evento decodificado más el handle nativo del broker para confirmarlo."""

    event: SaleEvent
    ack_handle: Any = None


class EventConsumer(ABC):
    """Consumidor asíncrono de eventos.

    El pipeline confirma en lote (`commit`) después de persistir las ventanas
    cerradas. Si el proceso cae antes de confirmar, el broker reentrega y el
    upsert idempotente en Postgres absorbe los duplicados.
    """

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    def events(self) -> AsyncIterator[ReceivedEvent]: ...

    @abstractmethod
    async def commit(self, pending: Sequence[ReceivedEvent]) -> None: ...

    async def __aenter__(self) -> EventConsumer:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

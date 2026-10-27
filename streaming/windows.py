"""Agregación de métricas por ventanas de tiempo (event time).

Se soportan dos tipos de ventana:

- Tumbling: tamaño fijo, sin solapamiento. Cada evento cae en exactamente una.
- Sliding (hopping): tamaño `size` que avanza cada `step`. Un evento cae en
  `size / step` ventanas.

Una tumbling es una sliding con `step == size`, por eso ambas comparten la
misma implementación.

Las ventanas se cierran con un watermark de event time:
    watermark = max(event_ts visto) - allowed_lateness
Una ventana [start, end) se emite cuando `end <= watermark`. Los eventos que
llegan para una ventana ya emitida se descartan y se cuentan en `late_events`.
Si el flujo se queda quieto, `close_expired(now)` permite avanzar el
watermark con el reloj de procesamiento.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class WindowStats:
    count: int
    total: float
    avg: float
    p95: float
    min: float
    max: float


@dataclass(frozen=True, slots=True)
class WindowResult:
    window_type: str
    window_start: datetime
    window_end: datetime
    group_key: str
    stats: WindowStats

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_type": self.window_type,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "group_key": self.group_key,
            **asdict(self.stats),
        }


def compute_stats(values: np.ndarray) -> WindowStats:
    """Métricas de una ventana con NumPy. p95 con interpolación lineal."""
    if values.size == 0:
        raise ValueError("No se pueden calcular métricas de una ventana vacía")
    return WindowStats(
        count=int(values.size),
        total=round(float(values.sum()), 2),
        avg=round(float(values.mean()), 2),
        p95=round(float(np.percentile(values, 95)), 2),
        min=round(float(values.min()), 2),
        max=round(float(values.max()), 2),
    )


def _to_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


class WindowAggregator:
    """Acumula valores por (inicio de ventana, grupo) y emite las ventanas cerradas."""

    ALL_GROUP = "all"

    def __init__(
        self,
        size: timedelta,
        step: timedelta | None = None,
        allowed_lateness: timedelta = timedelta(0),
        window_type: str | None = None,
        include_total_group: bool = True,
    ) -> None:
        step = step or size
        if size <= timedelta(0) or step <= timedelta(0):
            raise ValueError("size y step deben ser positivos")
        if step > size:
            raise ValueError("step no puede ser mayor que size (quedarían huecos)")
        if size % step != timedelta(0):
            raise ValueError("size debe ser múltiplo de step")

        self.size = size
        self.step = step
        self.allowed_lateness = allowed_lateness
        self.window_type = window_type or ("tumbling" if step == size else "sliding")
        self.include_total_group = include_total_group

        self._buffers: dict[tuple[datetime, str], list[float]] = defaultdict(list)
        self._max_event_ts: datetime | None = None
        # Fin de la última ventana emitida: todo lo que termine antes ya se cerró
        self._closed_until: datetime | None = None
        self.late_events = 0

    @property
    def watermark(self) -> datetime | None:
        if self._max_event_ts is None:
            return None
        return self._max_event_ts - self.allowed_lateness

    def window_starts_for(self, ts: datetime) -> list[datetime]:
        """Inicios de todas las ventanas que contienen `ts` (intervalos [start, end))."""
        ts = _to_utc(ts)
        offset = (ts - _EPOCH) % self.step
        last_start = ts - offset
        n_windows = self.size // self.step
        return [last_start - i * self.step for i in range(n_windows - 1, -1, -1)]

    def add(self, ts: datetime, value: float, group: str | None = None) -> bool:
        """Registra un evento. Devuelve False si se descartó por tardío."""
        ts = _to_utc(ts)
        starts = [s for s in self.window_starts_for(ts) if not self._is_closed(s)]
        if not starts:
            self.late_events += 1
            return False

        groups = [self.ALL_GROUP] if self.include_total_group else []
        if group is not None:
            groups.append(group)
        for start in starts:
            for g in groups:
                self._buffers[(start, g)].append(float(value))

        if self._max_event_ts is None or ts > self._max_event_ts:
            self._max_event_ts = ts
        return True

    def close_expired(self, now: datetime | None = None) -> list[WindowResult]:
        """Emite las ventanas cuyo fin quedó detrás del watermark.

        Si se pasa `now` (reloj de procesamiento) se usa el mayor entre el
        watermark de eventos y `now - allowed_lateness`.
        """
        candidates = [w for w in (self.watermark, _to_utc(now) - self.allowed_lateness if now else None) if w]
        if not candidates:
            return []
        watermark = max(candidates)
        ready = sorted(k for k in self._buffers if k[0] + self.size <= watermark)
        results = [self._emit(k) for k in ready]
        self._advance_closed(watermark)
        return results

    def flush(self) -> list[WindowResult]:
        """Emite todas las ventanas abiertas (por ejemplo al apagar el consumidor)."""
        results = [self._emit(k) for k in sorted(self._buffers)]
        if results:
            self._closed_until = max(r.window_end for r in results)
        return results

    def open_windows(self) -> int:
        return len({start for start, _ in self._buffers})

    def _emit(self, key: tuple[datetime, str]) -> WindowResult:
        start, group = key
        values = np.asarray(self._buffers.pop(key), dtype="float64")
        return WindowResult(
            window_type=self.window_type,
            window_start=start,
            window_end=start + self.size,
            group_key=group,
            stats=compute_stats(values),
        )

    def _advance_closed(self, watermark: datetime) -> None:
        # Mayor fin de ventana alineado que es <= watermark
        offset = (watermark - _EPOCH) % self.step
        aligned_end = watermark - offset
        if self._closed_until is None or aligned_end > self._closed_until:
            self._closed_until = aligned_end

    def _is_closed(self, start: datetime) -> bool:
        return self._closed_until is not None and start + self.size <= self._closed_until

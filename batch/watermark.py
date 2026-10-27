"""Lógica de watermark para cargas incrementales.

La idea es extraer siempre un intervalo semiabierto (lower, upper] sobre la
columna `updated_at` del origen:

- `lower` es el último watermark confirmado menos un lookback, para no perder
  filas que se commitearon tarde con un `updated_at` anterior al watermark.
- `upper` es el "ahora" menos un margen de seguridad, para no leer
  transacciones que todavía pueden estar en vuelo.

El solapamiento genera duplicados a propósito, el MERGE posterior los vuelve
idempotentes. El watermark nunca retrocede.

La parte pura (cálculo de ventanas) no depende de la base y se testea aislada,
el repositorio persiste el estado en `etl.watermark` de SQL Server.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy import Connection, text


@dataclass(frozen=True, slots=True)
class WatermarkState:
    entity: str
    value: datetime


@dataclass(frozen=True, slots=True)
class ExtractionWindow:
    lower: datetime  # exclusivo
    upper: datetime  # inclusivo

    @property
    def is_empty(self) -> bool:
        return self.lower >= self.upper


def ensure_utc(value: datetime) -> datetime:
    """Normaliza a UTC. Los datetimes naive se asumen ya en UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def compute_extraction_window(
    state: WatermarkState,
    now: datetime,
    lookback: timedelta = timedelta(minutes=10),
    safety_lag: timedelta = timedelta(seconds=30),
) -> ExtractionWindow:
    if lookback < timedelta(0) or safety_lag < timedelta(0):
        raise ValueError("lookback y safety_lag no pueden ser negativos")

    watermark = ensure_utc(state.value)
    upper = ensure_utc(now) - safety_lag
    return ExtractionWindow(lower=watermark - lookback, upper=upper)


def advance_watermark(current: datetime, extracted: pd.Series | None, window: ExtractionWindow) -> datetime:
    """Calcula el nuevo watermark a partir de los `updated_at` extraídos.

    Se toma el máximo observado (acotado por `window.upper`) y nunca se retrocede
    respecto del valor actual. Si no vino nada, el watermark queda igual: avanzar
    hasta `upper` sin datos podría saltear filas que se commitean tarde.
    """
    current = ensure_utc(current)
    if extracted is None or extracted.empty:
        return current

    values = pd.to_datetime(extracted, utc=True, errors="coerce", format="ISO8601").dropna()
    if values.empty:
        return current

    observed_max = min(values.max().to_pydatetime(), ensure_utc(window.upper))
    return max(current, observed_max)


class WatermarkRepository:
    """Persistencia del watermark en SQL Server (tabla `etl.watermark`)."""

    def __init__(self, initial_value: datetime) -> None:
        self._initial_value = ensure_utc(initial_value)

    def get(self, conn: Connection, entity: str) -> WatermarkState:
        row = conn.execute(
            text("SELECT watermark_value FROM etl.watermark WHERE entity_name = :entity"),
            {"entity": entity},
        ).first()
        if row is None:
            return WatermarkState(entity=entity, value=self._initial_value)
        return WatermarkState(entity=entity, value=ensure_utc(row[0]))

    def save(
        self,
        conn: Connection,
        entity: str,
        value: datetime,
        rows_extracted: int,
        status: str = "success",
    ) -> None:
        # Se ejecuta dentro de la misma transacción que el MERGE a raw,
        # así el watermark solo avanza si los datos quedaron persistidos.
        conn.execute(
            text(
                """
                MERGE etl.watermark WITH (HOLDLOCK) AS t
                USING (SELECT :entity AS entity_name) AS s
                   ON t.entity_name = s.entity_name
                WHEN MATCHED THEN UPDATE SET
                    watermark_value = :value,
                    last_run_at     = SYSUTCDATETIME(),
                    rows_extracted  = :rows,
                    last_status     = :status
                WHEN NOT MATCHED THEN
                    INSERT (entity_name, watermark_value, last_run_at, rows_extracted, last_status)
                    VALUES (:entity, :value, SYSUTCDATETIME(), :rows, :status);
                """
            ),
            {
                "entity": entity,
                # SQL Server guarda datetime2 sin zona, se persiste en UTC naive
                "value": ensure_utc(value).replace(tzinfo=None),
                "rows": rows_extracted,
                "status": status,
            },
        )

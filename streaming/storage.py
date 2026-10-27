"""Tabla de históricos de ventanas en PostgreSQL (SQLAlchemy Core)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from streaming.windows import WindowResult

if TYPE_CHECKING:
    # Solo para tipos: el módulo asyncio de SQLAlchemy exige greenlet al importarse
    from sqlalchemy.ext.asyncio import AsyncEngine

metadata = MetaData(schema="metrics")

window_metrics = Table(
    "window_metrics",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("window_type", Text, nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("window_end", DateTime(timezone=True), nullable=False),
    Column("group_key", Text, nullable=False),
    Column("event_count", Integer, nullable=False),
    Column("total_amount", Float, nullable=False),
    Column("avg_amount", Float, nullable=False),
    Column("p95_amount", Float, nullable=False),
    Column("min_amount", Float, nullable=False),
    Column("max_amount", Float, nullable=False),
    Column("computed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("window_type", "window_start", "window_end", "group_key", name="uq_window_metrics"),
)


def to_row(result: WindowResult) -> dict[str, Any]:
    s = result.stats
    return {
        "window_type": result.window_type,
        "window_start": result.window_start,
        "window_end": result.window_end,
        "group_key": result.group_key,
        "event_count": s.count,
        "total_amount": s.total,
        "avg_amount": s.avg,
        "p95_amount": s.p95,
        "min_amount": s.min,
        "max_amount": s.max,
    }


def row_to_dict(row: Any) -> dict[str, Any]:
    """Formato que consume el dashboard, igual al de los mensajes de Redis."""
    return {
        "window_type": row.window_type,
        "window_start": row.window_start.isoformat(),
        "window_end": row.window_end.isoformat(),
        "group_key": row.group_key,
        "count": row.event_count,
        "total": row.total_amount,
        "avg": row.avg_amount,
        "p95": row.p95_amount,
        "min": row.min_amount,
        "max": row.max_amount,
    }


class WindowMetricsRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def create_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS metrics")
            await conn.run_sync(metadata.create_all)

    async def upsert(self, results: Sequence[WindowResult]) -> None:
        """Upsert idempotente: reprocesar eventos reentregados reemplaza la ventana."""
        if not results:
            return
        stmt = pg_insert(window_metrics).values([to_row(r) for r in results])
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            constraint="uq_window_metrics",
            set_={
                "event_count": excluded.event_count,
                "total_amount": excluded.total_amount,
                "avg_amount": excluded.avg_amount,
                "p95_amount": excluded.p95_amount,
                "min_amount": excluded.min_amount,
                "max_amount": excluded.max_amount,
                "computed_at": func.now(),
            },
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def history(
        self,
        window_type: str,
        group_key: str | None = None,
        since: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        query = select(window_metrics).where(window_metrics.c.window_type == window_type)
        if group_key is not None:
            query = query.where(window_metrics.c.group_key == group_key)
        if since is not None:
            query = query.where(window_metrics.c.window_start >= since)
        query = query.order_by(window_metrics.c.window_start.desc()).limit(limit)

        async with self._engine.connect() as conn:
            rows = (await conn.execute(query)).all()
        # Se devuelve en orden cronológico, más cómodo para graficar
        return [row_to_dict(r) for r in reversed(rows)]

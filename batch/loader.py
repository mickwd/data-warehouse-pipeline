"""Carga en SQL Server: landing (truncate + bulk insert) y MERGE a raw."""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import Connection, text

from batch.entities import Entity
from batch.sql_builder import build_upsert_merge, quote_identifier

logger = logging.getLogger(__name__)


def to_sqlserver_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Adapta tipos para pyodbc: datetimes UTC naive y nulos de pandas a None."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out.astype(object).where(out.notna(), None)


class SqlServerLoader:
    def __init__(self, entity: Entity) -> None:
        self._entity = entity
        self._merge_sql = build_upsert_merge(
            target=entity.raw_table,
            source=entity.landing_table,
            keys=entity.natural_key,
            columns=entity.columns,
            version_column=entity.version_column,
        )

    def truncate_landing(self, conn: Connection) -> None:
        conn.execute(text(f"TRUNCATE TABLE {quote_identifier(self._entity.landing_table)}"))

    def load_landing(self, conn: Connection, df: pd.DataFrame, batch_id: str, extracted_at: datetime) -> int:
        if df.empty:
            return 0
        frame = to_sqlserver_frame(df[list(self._entity.columns)])
        frame["_batch_id"] = batch_id
        frame["_extracted_at"] = extracted_at.replace(tzinfo=None)

        schema, table = self._entity.landing_table.split(".")
        # El engine se crea con fast_executemany=True, inserta en bloques sin row-by-row
        frame.to_sql(table, conn, schema=schema, if_exists="append", index=False, chunksize=5_000)
        return len(frame)

    def merge_into_raw(self, conn: Connection) -> int:
        result = conn.execute(text(self._merge_sql))
        affected = result.rowcount or 0
        logger.info("merge_raw entity=%s affected=%s", self._entity.name, affected)
        return affected

"""Extractor incremental desde la base transaccional vía SQLAlchemy."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pandas as pd
from sqlalchemy import Engine, text

from batch.watermark import ExtractionWindow

logger = logging.getLogger(__name__)


class DatabaseExtractor:
    """Ejecuta la query de la entidad acotada por la ventana del watermark.

    Se lee con `stream_results` para no cargar todo el delta en memoria,
    en chunks de `chunk_size` filas.
    """

    def __init__(self, engine: Engine, query: str, chunk_size: int = 10_000) -> None:
        self._engine = engine
        self._query = text(query)
        self._chunk_size = chunk_size

    def extract(self, window: ExtractionWindow) -> Iterator[pd.DataFrame]:
        params = {"lower": window.lower, "upper": window.upper}
        with self._engine.connect().execution_options(stream_results=True) as conn:
            for i, chunk in enumerate(pd.read_sql_query(self._query, conn, params=params, chunksize=self._chunk_size)):
                logger.info("db_chunk index=%s rows=%s", i, len(chunk))
                yield chunk

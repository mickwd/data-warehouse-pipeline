"""Punto de entrada de la carga incremental de una entidad.

Uso:
    python -m batch.run --entity customers
    python -m batch.run --entity products --dry-run

Flujo por entidad:
    1. Lee el watermark de etl.watermark y calcula la ventana (lower, upper].
    2. Extrae el delta del origen (API o base) en chunks.
    3. Limpia cada chunk con Pandas/NumPy y lo inserta en landing.<entidad>.
    4. En una única transacción: MERGE landing -> raw y avance del watermark.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy import Engine, create_engine

from batch.entities import Entity, SourceKind, get_entity
from batch.extractors.base import Extractor
from batch.loader import SqlServerLoader
from batch.watermark import (
    WatermarkRepository,
    advance_watermark,
    compute_extraction_window,
)
from common.config import Settings, get_settings
from common.logs import configure_logging

logger = logging.getLogger("batch.run")


def build_dwh_engine(settings: Settings) -> Engine:
    return create_engine(settings.sqlserver.url, fast_executemany=True, pool_pre_ping=True)


def build_extractor(entity: Entity, settings: Settings) -> Extractor:
    if entity.source_kind is SourceKind.DATABASE:
        from batch.extractors.database import DatabaseExtractor

        assert entity.source_query is not None
        source_engine = create_engine(settings.source_db.url, pool_pre_ping=True)
        return DatabaseExtractor(source_engine, entity.source_query, settings.source_db.chunk_size)

    # Import diferido: httpx solo hace falta para entidades que vienen de una API
    from batch.extractors.rest_api import RestApiExtractor

    assert entity.api_path is not None
    api = settings.source_api
    return RestApiExtractor(
        api.base_url,
        entity.api_path,
        token=api.token,
        page_size=api.page_size,
        timeout_seconds=api.timeout_seconds,
        max_retries=api.max_retries,
        backoff_base_seconds=api.backoff_base_seconds,
    )


def _transformed_chunks(entity: Entity, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for chunk in chunks:
        cleaned = entity.transform(chunk)
        if not cleaned.empty:
            yield cleaned


def run_entity(entity_name: str, *, dry_run: bool = False, settings: Settings | None = None) -> dict[str, object]:
    settings = settings or get_settings()
    entity = get_entity(entity_name)
    batch_id = uuid.uuid4().hex
    started_at = datetime.now(UTC)

    engine = build_dwh_engine(settings)
    repo = WatermarkRepository(datetime.fromisoformat(settings.batch.initial_watermark))

    with engine.connect() as conn:
        state = repo.get(conn, entity.name)
    window = compute_extraction_window(
        state,
        now=started_at,
        lookback=timedelta(minutes=settings.batch.lookback_minutes),
        safety_lag=timedelta(seconds=settings.batch.safety_lag_seconds),
    )
    logger.info(
        "extract_start entity=%s batch_id=%s lower=%s upper=%s",
        entity.name,
        batch_id,
        window.lower.isoformat(),
        window.upper.isoformat(),
    )

    extractor = build_extractor(entity, settings)
    loader = SqlServerLoader(entity)
    rows_loaded = 0
    versions: list[pd.Series] = []

    # landing es efímera: truncate + inserts se commitean aparte del MERGE
    with engine.begin() as conn:
        if not dry_run:
            loader.truncate_landing(conn)
        for chunk in _transformed_chunks(entity, extractor.extract(window)):
            versions.append(chunk[entity.version_column])
            if not dry_run:
                rows_loaded += loader.load_landing(conn, chunk, batch_id, started_at)

    extracted_versions = pd.concat(versions) if versions else None
    new_watermark = advance_watermark(state.value, extracted_versions, window)

    merged = 0
    if not dry_run:
        with engine.begin() as conn:
            merged = loader.merge_into_raw(conn)
            repo.save(conn, entity.name, new_watermark, rows_extracted=rows_loaded)

    summary: dict[str, object] = {
        "entity": entity.name,
        "batch_id": batch_id,
        "rows_loaded": rows_loaded,
        "rows_merged": merged,
        "old_watermark": state.value.isoformat(),
        "new_watermark": new_watermark.isoformat(),
        "dry_run": dry_run,
    }
    logger.info("extract_done %s", " ".join(f"{k}={v}" for k, v in summary.items()))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga incremental de una entidad al DWH")
    parser.add_argument("--entity", required=True, help="customers | products | order_lines")
    parser.add_argument("--dry-run", action="store_true", help="Extrae y transforma sin escribir en el DWH")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    run_entity(args.entity, dry_run=args.dry_run, settings=settings)


if __name__ == "__main__":
    main()

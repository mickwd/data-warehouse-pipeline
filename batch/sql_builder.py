"""Generación de sentencias T-SQL a partir de la metadata de cada entidad."""

from __future__ import annotations

import re
from collections.abc import Sequence

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def quote_identifier(name: str) -> str:
    """Valida y cita un identificador (`schema.tabla` o `columna`) con corchetes."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Identificador inválido: {name!r}")
    return ".".join(f"[{part}]" for part in name.split("."))


def build_upsert_merge(
    target: str,
    source: str,
    keys: Sequence[str],
    columns: Sequence[str],
    version_column: str = "updated_at",
) -> str:
    """MERGE idempotente de landing a raw.

    - Inserta las claves nuevas.
    - Actualiza solo si la versión entrante es más nueva, así reprocesar un
      lote viejo (por el lookback o un retry) no pisa datos más recientes.
    - Agrega `_loaded_at` para auditoría.
    """
    if not keys:
        raise ValueError("Se necesita al menos una columna clave")
    missing = [k for k in (*keys, version_column) if k not in columns]
    if missing:
        raise ValueError(f"Columnas ausentes en la lista de columnas: {missing}")

    t, s = quote_identifier(target), quote_identifier(source)
    on_clause = " AND ".join(f"t.{quote_identifier(k)} = s.{quote_identifier(k)}" for k in keys)
    non_keys = [c for c in columns if c not in keys]
    set_clause = ",\n        ".join(f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in non_keys)
    insert_cols = ", ".join(quote_identifier(c) for c in columns)
    insert_vals = ", ".join(f"s.{quote_identifier(c)}" for c in columns)
    version = quote_identifier(version_column)
    partition = ", ".join(quote_identifier(k) for k in keys)

    # landing puede traer la misma clave en chunks distintos, se deduplica en el
    # USING porque MERGE falla si una fila destino matchea más de una fila origen
    return (
        f"MERGE {t} WITH (HOLDLOCK) AS t\n"
        f"USING (\n"
        f"    SELECT * FROM (\n"
        f"        SELECT src.*, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY src.{version} DESC) AS _rn\n"
        f"          FROM {s} AS src\n"
        f"    ) AS d WHERE d._rn = 1\n"
        f") AS s\n"
        f"   ON {on_clause}\n"
        f"WHEN MATCHED AND s.{version} > t.{version} THEN UPDATE SET\n"
        f"        {set_clause},\n"
        f"        t.[_loaded_at] = SYSUTCDATETIME()\n"
        f"WHEN NOT MATCHED BY TARGET THEN\n"
        f"    INSERT ({insert_cols}, [_loaded_at])\n"
        f"    VALUES ({insert_vals}, SYSUTCDATETIME());"
    )

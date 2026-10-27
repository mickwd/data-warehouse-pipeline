from __future__ import annotations

import pytest

from batch.entities import ENTITIES, get_entity
from batch.sql_builder import build_upsert_merge, quote_identifier


def test_quote_identifier() -> None:
    assert quote_identifier("raw.customers") == "[raw].[customers]"
    assert quote_identifier("updated_at") == "[updated_at]"


@pytest.mark.parametrize("bad", ["raw.customers; DROP TABLE x", "a.b.c", "1abc", "nombre con espacios"])
def test_quote_identifier_rejects_injection(bad: str) -> None:
    with pytest.raises(ValueError):
        quote_identifier(bad)


def test_merge_only_updates_newer_versions_and_dedupes_source() -> None:
    sql = build_upsert_merge(
        target="raw.order_lines",
        source="landing.order_lines",
        keys=["order_id", "line_number"],
        columns=["order_id", "line_number", "net_amount", "updated_at"],
    )
    assert "MERGE [raw].[order_lines] WITH (HOLDLOCK) AS t" in sql
    assert "ON t.[order_id] = s.[order_id] AND t.[line_number] = s.[line_number]" in sql
    assert "WHEN MATCHED AND s.[updated_at] > t.[updated_at] THEN UPDATE SET" in sql
    assert "PARTITION BY [order_id], [line_number] ORDER BY src.[updated_at] DESC" in sql
    # las claves no se actualizan, solo se insertan
    assert "t.[order_id] = s.[order_id]," not in sql
    assert "t.[net_amount] = s.[net_amount]" in sql
    assert sql.rstrip().endswith(";")


def test_merge_requires_keys_and_version_column() -> None:
    with pytest.raises(ValueError):
        build_upsert_merge("raw.a", "landing.a", keys=[], columns=["id"])
    with pytest.raises(ValueError):
        build_upsert_merge("raw.a", "landing.a", keys=["id"], columns=["id", "name"])


@pytest.mark.parametrize("name", sorted(ENTITIES))
def test_every_entity_produces_a_valid_merge(name: str) -> None:
    entity = get_entity(name)
    sql = build_upsert_merge(entity.raw_table, entity.landing_table, entity.natural_key, entity.columns)
    assert entity.raw_table.split(".")[1] in sql
    assert set(entity.natural_key) <= set(entity.columns)


def test_unknown_entity() -> None:
    with pytest.raises(ValueError, match="Entidad desconocida"):
        get_entity("inventario")

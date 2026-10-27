"""Catálogo de entidades que se cargan al DWH.

Cada entidad declara de dónde se extrae, cómo se limpia y cuál es su clave
natural. El resto del pipeline (loader, watermark, DAG) es genérico.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import pandas as pd

from batch import transform


class SourceKind(StrEnum):
    REST_API = "rest_api"
    DATABASE = "database"


@dataclass(frozen=True, slots=True)
class Entity:
    name: str
    source_kind: SourceKind
    natural_key: tuple[str, ...]
    columns: tuple[str, ...]
    transform: Callable[[pd.DataFrame], pd.DataFrame]
    # Para DATABASE: query parametrizada con :lower y :upper sobre el watermark
    source_query: str | None = None
    # Para REST_API: path del recurso paginado
    api_path: str | None = None
    version_column: str = "updated_at"

    @property
    def landing_table(self) -> str:
        return f"landing.{self.name}"

    @property
    def raw_table(self) -> str:
        return f"raw.{self.name}"


CUSTOMERS = Entity(
    name="customers",
    source_kind=SourceKind.DATABASE,
    natural_key=("customer_id",),
    columns=(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "country_code",
        "segment",
        "created_at",
        "updated_at",
        "row_hash",
    ),
    transform=transform.clean_customers,
    source_query="""
        SELECT customer_id, first_name, last_name, email, city, country, segment,
               created_at, updated_at
          FROM public.customers
         WHERE updated_at >  :lower
           AND updated_at <= :upper
         ORDER BY updated_at
    """,
)

ORDER_LINES = Entity(
    name="order_lines",
    source_kind=SourceKind.DATABASE,
    natural_key=("order_id", "line_number"),
    columns=(
        "order_id",
        "line_number",
        "customer_id",
        "product_id",
        "order_ts",
        "status",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "updated_at",
    ),
    transform=transform.clean_order_lines,
    # El trigger del OLTP actualiza orders.updated_at cuando cambia cualquier ítem,
    # por eso alcanza con filtrar por la cabecera (y el filtro usa el índice).
    source_query="""
        SELECT o.order_id, i.line_number, o.customer_id, i.product_id, o.order_ts,
               o.status, i.quantity, i.unit_price, i.discount_pct, o.updated_at
          FROM public.orders o
          JOIN public.order_items i ON i.order_id = o.order_id
         WHERE o.updated_at >  :lower
           AND o.updated_at <= :upper
         ORDER BY o.updated_at
    """,
)

PRODUCTS = Entity(
    name="products",
    source_kind=SourceKind.REST_API,
    natural_key=("product_id",),
    columns=(
        "product_id",
        "name",
        "category",
        "brand",
        "list_price",
        "currency",
        "is_active",
        "updated_at",
        "row_hash",
    ),
    transform=transform.clean_products,
    api_path="/api/v1/products",
)

ENTITIES: dict[str, Entity] = {e.name: e for e in (CUSTOMERS, PRODUCTS, ORDER_LINES)}


def get_entity(name: str) -> Entity:
    try:
        return ENTITIES[name]
    except KeyError as exc:
        raise ValueError(f"Entidad desconocida '{name}'. Disponibles: {sorted(ENTITIES)}") from exc

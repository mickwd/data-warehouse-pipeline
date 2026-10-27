"""Transformaciones de limpieza previas a la carga en landing.

Acá solo hay limpieza técnica (tipos, normalización, deduplicación, hash de
fila). Las reglas de negocio del modelo dimensional viven en dbt.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import numpy as np
import pandas as pd

VALID_ORDER_STATUSES = frozenset({"pending", "paid", "shipped", "delivered", "cancelled", "returned"})
_STATUS_ALIASES = {
    "canceled": "cancelled",
    "cancelado": "cancelled",
    "pagado": "paid",
    "enviado": "shipped",
    "entregado": "delivered",
    "devuelto": "returned",
    "pendiente": "pending",
}
_COUNTRY_ALIASES = {
    "argentina": "AR",
    "arg": "AR",
    "uruguay": "UY",
    "ury": "UY",
    "chile": "CL",
    "chl": "CL",
    "brasil": "BR",
    "brazil": "BR",
    "bra": "BR",
    "mexico": "MX",
    "méxico": "MX",
    "mex": "MX",
}


def _strip_or_none(series: pd.Series) -> pd.Series:
    """Trim de strings, los vacíos pasan a nulo."""
    cleaned = series.astype("string").str.strip()
    return cleaned.mask(cleaned == "", pd.NA)


def deduplicate_latest(df: pd.DataFrame, keys: Sequence[str], version_col: str = "updated_at") -> pd.DataFrame:
    """Deja una sola fila por clave natural, la de mayor `version_col`.

    Por el lookback del watermark es normal que la misma fila llegue más de una vez.
    """
    if df.empty:
        return df.copy()
    ordered = df.sort_values([*keys, version_col], kind="mergesort")
    return ordered.drop_duplicates(subset=list(keys), keep="last").reset_index(drop=True)


def add_row_hash(df: pd.DataFrame, columns: Sequence[str], target: str = "row_hash") -> pd.DataFrame:
    """Agrega un hash MD5 determinístico de los atributos de negocio.

    dbt lo usa para detectar cambios en las dimensiones SCD2 sin comparar
    columna por columna. Los nulos se representan con un marcador fijo para
    distinguir NULL de string vacío.
    """
    out = df.copy()
    if out.empty:
        out[target] = pd.Series(dtype="string")
        return out

    normalized = out[list(columns)].astype("string").fillna("\x00NULL\x00")
    joined = normalized.agg("|".join, axis=1)
    out[target] = joined.map(lambda value: hashlib.md5(value.encode("utf-8")).hexdigest())
    return out


def normalize_country(series: pd.Series) -> pd.Series:
    cleaned = _strip_or_none(series)
    lowered = cleaned.str.lower()
    mapped = lowered.map(_COUNTRY_ALIASES)
    # Si ya es un ISO-2 se respeta en mayúsculas, si no se reconoce queda nulo
    iso2 = cleaned.where(cleaned.str.len() == 2).str.upper()
    return mapped.fillna(iso2).astype("string")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = pd.to_numeric(out["customer_id"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["customer_id"])

    out["first_name"] = _strip_or_none(out["first_name"]).str.title()
    out["last_name"] = _strip_or_none(out["last_name"]).str.title()
    out["email"] = _strip_or_none(out["email"]).str.lower()
    out["city"] = _strip_or_none(out["city"]).str.title()
    out["country_code"] = normalize_country(out["country"])
    out["segment"] = _strip_or_none(out["segment"]).str.lower().fillna("unknown")
    out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce", format="ISO8601")
    out["updated_at"] = pd.to_datetime(out["updated_at"], utc=True, errors="coerce", format="ISO8601")
    out = out.dropna(subset=["updated_at"])

    out = deduplicate_latest(out, ["customer_id"])
    out = add_row_hash(out, ["first_name", "last_name", "email", "city", "country_code", "segment"])
    return out[
        [
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
        ]
    ]


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["product_id"] = _strip_or_none(out["product_id"]).str.upper()
    out = out.dropna(subset=["product_id"])

    out["name"] = _strip_or_none(out["name"])
    out["category"] = _strip_or_none(out["category"]).str.lower().fillna("uncategorized")
    out["brand"] = _strip_or_none(out["brand"])
    price = pd.to_numeric(out["list_price"], errors="coerce").astype("float64")
    # Un precio negativo es un error de origen, se deja nulo y lo marca el test de dbt
    out["list_price"] = price.where(price >= 0).round(2)
    out["currency"] = _strip_or_none(out["currency"]).str.upper().fillna("ARS")
    out["is_active"] = out["is_active"].map(_to_bool).astype("boolean")
    out["updated_at"] = pd.to_datetime(out["updated_at"], utc=True, errors="coerce", format="ISO8601")
    out = out.dropna(subset=["updated_at"])

    out = deduplicate_latest(out, ["product_id"])
    out = add_row_hash(out, ["name", "category", "brand", "list_price", "currency", "is_active"])
    return out[
        ["product_id", "name", "category", "brand", "list_price", "currency", "is_active", "updated_at", "row_hash"]
    ]


def clean_order_lines(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("order_id", "line_number", "customer_id"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    out["product_id"] = _strip_or_none(out["product_id"]).str.upper()
    out = out.dropna(subset=["order_id", "line_number", "customer_id", "product_id"])

    quantity = pd.to_numeric(out["quantity"], errors="coerce").fillna(0).to_numpy(dtype="float64")
    unit_price = pd.to_numeric(out["unit_price"], errors="coerce").fillna(0).to_numpy(dtype="float64")
    discount = pd.to_numeric(out["discount_pct"], errors="coerce").fillna(0).to_numpy(dtype="float64")

    quantity = np.clip(quantity, 0, None)
    unit_price = np.clip(unit_price, 0, None)
    discount = np.clip(discount, 0.0, 1.0)

    gross = quantity * unit_price
    discount_amount = gross * discount

    out["quantity"] = quantity.astype("int64")
    out["unit_price"] = np.round(unit_price, 2)
    out["discount_pct"] = np.round(discount, 4)
    out["gross_amount"] = np.round(gross, 2)
    out["discount_amount"] = np.round(discount_amount, 2)
    out["net_amount"] = np.round(gross - discount_amount, 2)

    status = _strip_or_none(out["status"]).str.lower()
    status = status.replace(_STATUS_ALIASES)
    out["status"] = status.where(status.isin(VALID_ORDER_STATUSES), "unknown")

    out["order_ts"] = pd.to_datetime(out["order_ts"], utc=True, errors="coerce", format="ISO8601")
    out["updated_at"] = pd.to_datetime(out["updated_at"], utc=True, errors="coerce", format="ISO8601")
    out = out.dropna(subset=["order_ts", "updated_at"])

    out = deduplicate_latest(out, ["order_id", "line_number"])
    return out[
        [
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
        ]
    ]


def _to_bool(value: object) -> bool | None:
    if value is None or (isinstance(value, float) and np.isnan(value)) or value is pd.NA:
        return None
    if isinstance(value, bool | np.bool_):
        return bool(value)
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "t", "yes", "y", "si", "sí"}:
        return True
    if text_value in {"0", "false", "f", "no", "n"}:
        return False
    return None

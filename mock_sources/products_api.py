"""API REST de catálogo simulada, con paginación por cursor y filtro incremental.

Uso:
    uvicorn mock_sources.products_api:app --port 8001

Cada cierto tiempo "modifica" algunos productos (precio, estado) para que la
carga incremental tenga deltas reales. Devuelve 503 aleatorios para ejercitar
los reintentos del extractor.
"""

from __future__ import annotations

import base64
import random
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query

CATEGORIES = ["Electrónica", "Hogar", "Deportes", "Librería", "Juguetes", " almacén ", "Indumentaria"]
BRANDS = ["Acme", "Pampa", "Andes", "Litoral", None]
FAILURE_RATE = 0.05

app = FastAPI(title="Catálogo de productos (mock)")
_rng = random.Random(42)
_start = datetime.now(UTC) - timedelta(days=30)
_products: list[dict[str, Any]] = [
    {
        "product_id": f"p{i:05d}",  # minúscula a propósito, el transform la normaliza
        "name": f"Producto {i}",
        "category": _rng.choice(CATEGORIES),
        "brand": _rng.choice(BRANDS),
        "list_price": round(_rng.uniform(500, 250_000), 2),
        "currency": "ars",
        "is_active": _rng.random() > 0.05,
        "updated_at": (_start + timedelta(minutes=i)).isoformat(),
    }
    for i in range(1, 501)
]


def _mutate_some() -> None:
    now = datetime.now(UTC).isoformat()
    for product in _rng.sample(_products, k=5):
        product["list_price"] = round(product["list_price"] * _rng.uniform(0.9, 1.15), 2)
        product["is_active"] = _rng.random() > 0.05
        product["updated_at"] = now


def _encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="cursor inválido") from exc


@app.get("/api/v1/products")
def list_products(
    updated_since: datetime,
    updated_until: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    cursor: str | None = None,
    page: int | None = None,  # aceptado por compatibilidad, se ignora si hay cursor
) -> dict[str, Any]:
    if _rng.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="servicio temporalmente no disponible")

    if cursor is None:
        _mutate_some()
    until = updated_until or datetime.now(UTC)
    matching = sorted(
        (p for p in _products if updated_since < datetime.fromisoformat(p["updated_at"]) <= until),
        key=lambda p: (p["updated_at"], p["product_id"]),
    )
    offset = _decode_cursor(cursor) if cursor else 0
    page_items = matching[offset : offset + limit]
    next_offset = offset + limit
    return {
        "data": page_items,
        "next_cursor": _encode_cursor(next_offset) if next_offset < len(matching) else None,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

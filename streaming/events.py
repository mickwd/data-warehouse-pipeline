"""Esquema de los eventos de venta que viajan por el broker."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

Channel = Literal["web", "mobile", "store", "marketplace"]


class SaleEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str
    event_ts: datetime
    store_id: int = Field(ge=1)
    product_id: str
    channel: Channel
    quantity: int = Field(ge=1)
    amount: float = Field(ge=0)

    @field_validator("event_ts")
    @classmethod
    def _as_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes | str) -> SaleEvent:
        return cls.model_validate_json(raw)

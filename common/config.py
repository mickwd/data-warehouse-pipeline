"""Configuración centralizada del proyecto.

Cada bloque de settings lee sus variables de entorno con un prefijo propio,
así se pueden sobreescribir por servicio en docker-compose sin pisarse.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from urllib.parse import quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_BASE_CONFIG = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class SqlServerSettings(BaseSettings):
    """Data Warehouse en SQL Server."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="MSSQL_")

    host: str = "localhost"
    port: int = 1433
    database: str = "dwh"
    user: str = "sa"
    password: str = "ChangeMe_123!"
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_server_certificate: bool = True

    @property
    def url(self) -> str:
        trust = "yes" if self.trust_server_certificate else "no"
        return (
            f"mssql+pyodbc://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?driver={quote_plus(self.driver)}&TrustServerCertificate={trust}"
        )


class SourceDbSettings(BaseSettings):
    """Base transaccional de origen (OLTP) desde donde se hace la carga incremental."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="SOURCE_DB_")

    url: str = "postgresql+psycopg://shop:shop@localhost:5432/shop"
    chunk_size: int = 10_000


class SourceApiSettings(BaseSettings):
    """API REST de origen (catálogo de productos)."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="SOURCE_API_")

    base_url: str = "http://localhost:8001"
    token: str | None = None
    page_size: int = 200
    timeout_seconds: float = 30.0
    max_retries: int = 5
    backoff_base_seconds: float = 0.5


class BatchSettings(BaseSettings):
    """Parámetros de la carga incremental."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="BATCH_")

    # Solapamiento hacia atrás sobre el último watermark para tolerar commits tardíos
    lookback_minutes: int = 10
    # Margen respecto del "ahora" para no leer transacciones que todavía están abiertas
    safety_lag_seconds: int = 30
    initial_watermark: str = "1900-01-01T00:00:00+00:00"


class StreamSettings(BaseSettings):
    """Broker de eventos y parámetros de ventanas."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="STREAM_")

    broker: Literal["kafka", "rabbitmq"] = "kafka"
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "sales.events"
    kafka_group_id: str = "metrics-aggregator"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_exchange: str = "sales.events"
    rabbitmq_queue: str = "metrics-aggregator"

    tumbling_window_seconds: int = 10
    sliding_window_seconds: int = 60
    sliding_step_seconds: int = 10
    allowed_lateness_seconds: int = 5
    tick_seconds: float = 1.0

    producer_events_per_second: float = 25.0


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="REDIS_")

    url: str = "redis://localhost:6379/0"
    metrics_channel: str = "metrics:windows"


class MetricsDbSettings(BaseSettings):
    """PostgreSQL donde se persisten los históricos de ventanas."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="METRICS_DB_")

    url: str = "postgresql+asyncpg://metrics:metrics@localhost:5432/metrics"


class Settings(BaseSettings):
    model_config = _BASE_CONFIG

    env: Literal["local", "docker", "test"] = Field(default="local", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    sqlserver: SqlServerSettings = Field(default_factory=SqlServerSettings)
    source_db: SourceDbSettings = Field(default_factory=SourceDbSettings)
    source_api: SourceApiSettings = Field(default_factory=SourceApiSettings)
    batch: BatchSettings = Field(default_factory=BatchSettings)
    stream: StreamSettings = Field(default_factory=StreamSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    metrics_db: MetricsDbSettings = Field(default_factory=MetricsDbSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

-- Históricos de métricas por ventana (escritos por el consumidor de streaming).
-- La app también crea la tabla con SQLAlchemy si no existe, este script es la
-- versión de referencia con índices.
\connect metrics metrics

CREATE SCHEMA IF NOT EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics.window_metrics (
    id           BIGSERIAL PRIMARY KEY,
    window_type  TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    group_key    TEXT             NOT NULL,
    event_count  INTEGER          NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    avg_amount   DOUBLE PRECISION NOT NULL,
    p95_amount   DOUBLE PRECISION NOT NULL,
    min_amount   DOUBLE PRECISION NOT NULL,
    max_amount   DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT uq_window_metrics UNIQUE (window_type, window_start, window_end, group_key)
);

CREATE INDEX IF NOT EXISTS ix_window_metrics_type_start
    ON metrics.window_metrics (window_type, window_start DESC);

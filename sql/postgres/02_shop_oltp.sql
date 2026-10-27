-- Esquema transaccional de origen (simula el sistema de ventas)
\connect shop shop

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name  TEXT        NOT NULL,
    last_name   TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    city        TEXT,
    country     TEXT,
    segment     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id    BIGSERIAL PRIMARY KEY,
    customer_id BIGINT      NOT NULL REFERENCES public.customers (customer_id),
    order_ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
    status      TEXT        NOT NULL DEFAULT 'pending',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_id     BIGINT        NOT NULL REFERENCES public.orders (order_id) ON DELETE CASCADE,
    line_number  INT           NOT NULL,
    product_id   TEXT          NOT NULL,
    quantity     INT           NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(18,2) NOT NULL,
    discount_pct NUMERIC(5,4)  NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_number)
);

-- Índices sobre la columna de watermark
CREATE INDEX IF NOT EXISTS ix_customers_updated_at ON public.customers (updated_at);
CREATE INDEX IF NOT EXISTS ix_orders_updated_at    ON public.orders (updated_at);

-- updated_at mantenido por la base, no por la aplicación
CREATE OR REPLACE FUNCTION public.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Cualquier cambio en un ítem "toca" la cabecera para que entre en el delta
CREATE OR REPLACE FUNCTION public.touch_order_header() RETURNS trigger AS $$
BEGIN
    UPDATE public.orders SET updated_at = clock_timestamp()
     WHERE order_id = COALESCE(NEW.order_id, OLD.order_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_touch ON public.customers;
CREATE TRIGGER trg_customers_touch BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS trg_orders_touch ON public.orders;
CREATE TRIGGER trg_orders_touch BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS trg_order_items_touch_header ON public.order_items;
CREATE TRIGGER trg_order_items_touch_header AFTER INSERT OR UPDATE OR DELETE ON public.order_items
    FOR EACH ROW EXECUTE FUNCTION public.touch_order_header();

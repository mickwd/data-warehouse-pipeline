-- =============================================================================
-- Modelo dimensional (esquema estrella).
-- Las tablas se crean de antemano con PKs, FKs e índices, y los modelos de dbt
-- son incrementales (merge), así dbt nunca las recrea salvo un --full-refresh.
-- Surrogate keys: hash MD5 hex (CHAR(32)) generado en dbt, determinístico y
-- reproducible entre entornos. dim_date usa la clave inteligente yyyymmdd.
-- =============================================================================
USE dwh;
GO

IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL
CREATE TABLE dw.dim_date (
    date_key       INT          NOT NULL,
    full_date      DATE         NOT NULL,
    year_number    SMALLINT     NOT NULL,
    quarter_number TINYINT      NOT NULL,
    month_number   TINYINT      NOT NULL,
    month_name     NVARCHAR(12) NOT NULL,
    day_of_month   TINYINT      NOT NULL,
    day_of_week    TINYINT      NOT NULL,  -- 1 = lunes ... 7 = domingo (ISO)
    day_name       NVARCHAR(12) NOT NULL,
    iso_week       TINYINT      NOT NULL,
    is_weekend     BIT          NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY CLUSTERED (date_key)
);

-- SCD tipo 2: una fila por versión de cada cliente
IF OBJECT_ID(N'dw.dim_customer', N'U') IS NULL
CREATE TABLE dw.dim_customer (
    customer_sk       CHAR(32)      NOT NULL,
    customer_id       BIGINT        NOT NULL,
    full_name         NVARCHAR(201) NULL,
    email             NVARCHAR(320) NULL,
    city              NVARCHAR(100) NULL,
    country_code      CHAR(2)       NULL,
    segment           NVARCHAR(50)  NULL,
    row_hash          CHAR(32)      NOT NULL,
    valid_from        DATETIME2(6)  NOT NULL,
    valid_to          DATETIME2(6)  NOT NULL,
    is_current        BIT           NOT NULL,
    source_updated_at DATETIME2(6)  NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY CLUSTERED (customer_sk)
);
GO

-- Garantiza una sola versión vigente por cliente
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ux_dim_customer_current')
    CREATE UNIQUE INDEX ux_dim_customer_current ON dw.dim_customer (customer_id) WHERE is_current = 1;
-- Lookup de la versión vigente a la fecha de la venta
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_dim_customer_validity')
    CREATE INDEX ix_dim_customer_validity ON dw.dim_customer (customer_id, valid_from, valid_to) INCLUDE (customer_sk);
GO

-- SCD tipo 1: se sobreescriben los atributos
IF OBJECT_ID(N'dw.dim_product', N'U') IS NULL
CREATE TABLE dw.dim_product (
    product_sk        CHAR(32)       NOT NULL,
    product_id        NVARCHAR(32)   NOT NULL,
    name              NVARCHAR(200)  NULL,
    category          NVARCHAR(100)  NOT NULL,
    brand             NVARCHAR(100)  NULL,
    list_price        DECIMAL(18, 2) NULL,
    currency          CHAR(3)        NOT NULL,
    is_active         BIT            NOT NULL,
    row_hash          CHAR(32)       NOT NULL,
    source_updated_at DATETIME2(6)   NOT NULL,
    CONSTRAINT pk_dim_product PRIMARY KEY CLUSTERED (product_sk),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id)
);
GO

-- Hecho a nivel línea de pedido (grano: una fila por order_id + line_number)
IF OBJECT_ID(N'dw.fact_sales', N'U') IS NULL
CREATE TABLE dw.fact_sales (
    order_line_key    CHAR(32)       NOT NULL,
    order_id          BIGINT         NOT NULL,
    line_number       INT            NOT NULL,
    date_key          INT            NOT NULL,
    customer_sk       CHAR(32)       NOT NULL,
    product_sk        CHAR(32)       NOT NULL,
    order_ts          DATETIME2(6)   NOT NULL,
    status            NVARCHAR(20)   NOT NULL,
    quantity          INT            NOT NULL,
    unit_price        DECIMAL(18, 2) NOT NULL,
    discount_pct      DECIMAL(5, 4)  NOT NULL,
    gross_amount      DECIMAL(18, 2) NOT NULL,
    discount_amount   DECIMAL(18, 2) NOT NULL,
    net_amount        DECIMAL(18, 2) NOT NULL,
    source_updated_at DATETIME2(6)   NOT NULL,
    CONSTRAINT pk_fact_sales PRIMARY KEY NONCLUSTERED (order_line_key),
    CONSTRAINT fk_fact_sales_date     FOREIGN KEY (date_key)    REFERENCES dw.dim_date (date_key),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_sk) REFERENCES dw.dim_customer (customer_sk),
    CONSTRAINT fk_fact_sales_product  FOREIGN KEY (product_sk)  REFERENCES dw.dim_product (product_sk)
);
GO

-- Columnstore para las consultas analíticas (scans + agregaciones)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'cci_fact_sales')
    CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_sales ON dw.fact_sales;
GO

USE dwh;
GO

-- -----------------------------------------------------------------------------
-- landing: se trunca en cada corrida. Heap sin índices para insertar rápido.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'landing.customers', N'U') IS NULL
CREATE TABLE landing.customers (
    customer_id   BIGINT         NOT NULL,
    first_name    NVARCHAR(100)  NULL,
    last_name     NVARCHAR(100)  NULL,
    email         NVARCHAR(320)  NULL,
    city          NVARCHAR(100)  NULL,
    country_code  CHAR(2)        NULL,
    segment       NVARCHAR(50)   NULL,
    created_at    DATETIME2(6)   NULL,
    updated_at    DATETIME2(6)   NOT NULL,
    row_hash      CHAR(32)       NOT NULL,
    _batch_id     CHAR(32)       NOT NULL,
    _extracted_at DATETIME2(3)   NOT NULL
);

IF OBJECT_ID(N'landing.products', N'U') IS NULL
CREATE TABLE landing.products (
    product_id    NVARCHAR(32)   NOT NULL,
    name          NVARCHAR(200)  NULL,
    category      NVARCHAR(100)  NULL,
    brand         NVARCHAR(100)  NULL,
    list_price    DECIMAL(18, 2) NULL,
    currency      CHAR(3)        NULL,
    is_active     BIT            NULL,
    updated_at    DATETIME2(6)   NOT NULL,
    row_hash      CHAR(32)       NOT NULL,
    _batch_id     CHAR(32)       NOT NULL,
    _extracted_at DATETIME2(3)   NOT NULL
);

IF OBJECT_ID(N'landing.order_lines', N'U') IS NULL
CREATE TABLE landing.order_lines (
    order_id        BIGINT         NOT NULL,
    line_number     INT            NOT NULL,
    customer_id     BIGINT         NOT NULL,
    product_id      NVARCHAR(32)   NOT NULL,
    order_ts        DATETIME2(6)   NOT NULL,
    status          NVARCHAR(20)   NOT NULL,
    quantity        INT            NOT NULL,
    unit_price      DECIMAL(18, 2) NOT NULL,
    discount_pct    DECIMAL(5, 4)  NOT NULL,
    gross_amount    DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_amount      DECIMAL(18, 2) NOT NULL,
    updated_at      DATETIME2(6)   NOT NULL,
    _batch_id       CHAR(32)       NOT NULL,
    _extracted_at   DATETIME2(3)   NOT NULL
);
GO

-- -----------------------------------------------------------------------------
-- raw: última versión conocida de cada fila de origen, clave natural como PK.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'raw.customers', N'U') IS NULL
CREATE TABLE raw.customers (
    customer_id  BIGINT        NOT NULL,
    first_name   NVARCHAR(100) NULL,
    last_name    NVARCHAR(100) NULL,
    email        NVARCHAR(320) NULL,
    city         NVARCHAR(100) NULL,
    country_code CHAR(2)       NULL,
    segment      NVARCHAR(50)  NULL,
    created_at   DATETIME2(6)  NULL,
    updated_at   DATETIME2(6)  NOT NULL,
    row_hash     CHAR(32)      NOT NULL,
    _loaded_at   DATETIME2(3)  NOT NULL,
    CONSTRAINT pk_raw_customers PRIMARY KEY CLUSTERED (customer_id)
);

IF OBJECT_ID(N'raw.products', N'U') IS NULL
CREATE TABLE raw.products (
    product_id NVARCHAR(32)   NOT NULL,
    name       NVARCHAR(200)  NULL,
    category   NVARCHAR(100)  NULL,
    brand      NVARCHAR(100)  NULL,
    list_price DECIMAL(18, 2) NULL,
    currency   CHAR(3)        NULL,
    is_active  BIT            NULL,
    updated_at DATETIME2(6)   NOT NULL,
    row_hash   CHAR(32)       NOT NULL,
    _loaded_at DATETIME2(3)   NOT NULL,
    CONSTRAINT pk_raw_products PRIMARY KEY CLUSTERED (product_id)
);

IF OBJECT_ID(N'raw.order_lines', N'U') IS NULL
CREATE TABLE raw.order_lines (
    order_id        BIGINT         NOT NULL,
    line_number     INT            NOT NULL,
    customer_id     BIGINT         NOT NULL,
    product_id      NVARCHAR(32)   NOT NULL,
    order_ts        DATETIME2(6)   NOT NULL,
    status          NVARCHAR(20)   NOT NULL,
    quantity        INT            NOT NULL,
    unit_price      DECIMAL(18, 2) NOT NULL,
    discount_pct    DECIMAL(5, 4)  NOT NULL,
    gross_amount    DECIMAL(18, 2) NOT NULL,
    discount_amount DECIMAL(18, 2) NOT NULL,
    net_amount      DECIMAL(18, 2) NOT NULL,
    updated_at      DATETIME2(6)   NOT NULL,
    _loaded_at      DATETIME2(3)   NOT NULL,
    CONSTRAINT pk_raw_order_lines PRIMARY KEY CLUSTERED (order_id, line_number)
);

-- dbt filtra incrementalmente por updated_at
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_raw_order_lines_updated_at')
    CREATE INDEX ix_raw_order_lines_updated_at ON raw.order_lines (updated_at);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_raw_customers_updated_at')
    CREATE INDEX ix_raw_customers_updated_at ON raw.customers (updated_at);
GO

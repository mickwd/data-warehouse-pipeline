-- Miembros "desconocido" para que el hecho nunca quede con FK huérfana
-- (ventas que llegan antes que su cliente o producto, early arriving facts).
USE dwh;
GO

DECLARE @unknown_sk CHAR(32) = REPLICATE('0', 32);

IF NOT EXISTS (SELECT 1 FROM dw.dim_customer WHERE customer_sk = @unknown_sk)
    INSERT INTO dw.dim_customer
        (customer_sk, customer_id, full_name, email, city, country_code, segment,
         row_hash, valid_from, valid_to, is_current, source_updated_at)
    VALUES
        (@unknown_sk, -1, N'Desconocido', NULL, NULL, NULL, N'unknown',
         @unknown_sk, '1900-01-01', '9999-12-31', 1, '1900-01-01');

IF NOT EXISTS (SELECT 1 FROM dw.dim_product WHERE product_sk = @unknown_sk)
    INSERT INTO dw.dim_product
        (product_sk, product_id, name, category, brand, list_price, currency,
         is_active, row_hash, source_updated_at)
    VALUES
        (@unknown_sk, N'UNKNOWN', N'Desconocido', N'uncategorized', NULL, NULL, 'ARS',
         0, @unknown_sk, '1900-01-01');
GO

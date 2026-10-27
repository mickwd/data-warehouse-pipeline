{{ config(unique_key='product_sk') }}

-- Dimensión de productos SCD tipo 1: los cambios de precio o estado
-- sobreescriben la fila. El histórico de precios queda en fact_sales.unit_price.
select
    {{ surrogate_key(['product_id']) }} as product_sk,
    product_id,
    name,
    category,
    brand,
    list_price,
    currency,
    is_active,
    row_hash,
    updated_at as source_updated_at
from {{ ref('stg_products') }}
{% if is_incremental() %}
where updated_at > {{ incremental_high_watermark('source_updated_at') }}
{% endif %}

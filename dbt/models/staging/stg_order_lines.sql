select
    order_id,
    line_number,
    customer_id,
    product_id,
    order_ts,
    -- Clave inteligente yyyymmdd para dim_date
    year(order_ts) * 10000 + month(order_ts) * 100 + day(order_ts) as date_key,
    status,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    discount_amount,
    net_amount,
    updated_at
from {{ source('raw', 'order_lines') }}

select
    product_id,
    name,
    coalesce(category, 'uncategorized') as category,
    brand,
    list_price,
    coalesce(currency, 'ARS') as currency,
    cast(coalesce(is_active, 0) as bit) as is_active,
    row_hash,
    updated_at
from {{ source('raw', 'products') }}

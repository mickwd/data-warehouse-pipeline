-- Clientes listos para la dimensión. La limpieza técnica ya la hizo Pandas,
-- acá se arman los atributos derivados que consume dim_customer.
select
    customer_id,
    cast(nullif(concat_ws(' ', first_name, last_name), '') as nvarchar(201)) as full_name,
    email,
    city,
    country_code,
    coalesce(segment, 'unknown') as segment,
    row_hash,
    created_at,
    updated_at
from {{ source('raw', 'customers') }}

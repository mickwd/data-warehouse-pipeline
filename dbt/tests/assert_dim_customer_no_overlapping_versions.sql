-- SCD2: los intervalos [valid_from, valid_to) de un mismo cliente no se pisan
-- y cada versión cerrada termina exactamente donde empieza la siguiente.
with ordered as (
    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by customer_id order by valid_from) as next_valid_from
    from {{ ref('dim_customer') }}
)
select *
from ordered
where valid_from >= valid_to
   or (next_valid_from is not null and next_valid_from <> valid_to)

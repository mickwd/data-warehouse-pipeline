{{ config(unique_key='order_line_key') }}

{#
  Hecho de ventas, grano línea de pedido.
  - Cliente: versión SCD2 vigente al momento de la venta (order_ts).
  - Producto: SCD1, fila actual.
  - Si la dimensión todavía no llegó se asigna el miembro desconocido. El
    lookback de `fact_lookback_hours` reprocesa esas líneas en las corridas
    siguientes y el MERGE las corrige cuando la dimensión ya existe.
#}

with lines as (
    select *
    from {{ ref('stg_order_lines') }}
    {% if is_incremental() %}
    where updated_at > {{ incremental_high_watermark('source_updated_at', var('fact_lookback_hours')) }}
    {% endif %}
)

select
    {{ surrogate_key(['l.order_id', 'l.line_number']) }} as order_line_key,
    l.order_id,
    l.line_number,
    l.date_key,
    coalesce(c.customer_sk, '{{ var("unknown_sk") }}') as customer_sk,
    coalesce(p.product_sk, '{{ var("unknown_sk") }}') as product_sk,
    l.order_ts,
    l.status,
    l.quantity,
    l.unit_price,
    l.discount_pct,
    l.gross_amount,
    l.discount_amount,
    l.net_amount,
    l.updated_at as source_updated_at
from lines l
left join {{ ref('dim_customer') }} c
    on  c.customer_id = l.customer_id
    and l.order_ts >= c.valid_from
    and l.order_ts <  c.valid_to
left join {{ ref('dim_product') }} p
    on p.product_id = l.product_id

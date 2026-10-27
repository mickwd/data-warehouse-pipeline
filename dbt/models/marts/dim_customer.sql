{{ config(unique_key='customer_sk') }}

{#
  Dimensión de clientes SCD tipo 2.

  En cada corrida incremental:
    1. Se toman los clientes de staging modificados desde la última carga.
    2. Se comparan contra la versión vigente por row_hash (calculado en Pandas).
    3. Para los que cambiaron se emiten dos filas:
         - la versión vigente actual, cerrada (valid_to = updated_at nuevo, is_current = 0)
         - la nueva versión vigente (valid_to = 9999-12-31, is_current = 1)
  El MERGE por customer_sk actualiza la primera e inserta la segunda.

  La primera versión de cada cliente arranca en 1900-01-01 para que las ventas
  anteriores a su primera extracción también encuentren una versión válida.
  Si un cliente cambia más de una vez entre corridas se conserva solo el último
  estado, raw guarda la foto actual y no el historial completo del OLTP.
#}

with source as (
    select *
    from {{ ref('stg_customers') }}
    {% if is_incremental() %}
    where updated_at > {{ incremental_high_watermark('source_updated_at') }}
    {% endif %}
),

{% if is_incremental() %}
current_versions as (
    select *
    from {{ this }}
    where is_current = 1
),
{% endif %}

changed as (
    select
        s.*,
        {% if is_incremental() %}
        case when c.customer_id is null then 1 else 0 end as is_first_version
        {% else %}
        1 as is_first_version
        {% endif %}
    from source s
    {% if is_incremental() %}
    left join current_versions c
        on c.customer_id = s.customer_id
    -- Un update en el OLTP sin cambios en atributos de negocio no genera versión
    where c.customer_id is null
       or c.row_hash <> s.row_hash
    {% endif %}
),

new_versions as (
    select
        {{ surrogate_key(['customer_id', 'updated_at']) }} as customer_sk,
        customer_id,
        full_name,
        email,
        city,
        country_code,
        segment,
        row_hash,
        case when is_first_version = 1
             then cast('1900-01-01' as datetime2(6))
             else updated_at
        end as valid_from,
        cast('9999-12-31' as datetime2(6)) as valid_to,
        cast(1 as bit) as is_current,
        updated_at as source_updated_at
    from changed
)

{% if is_incremental() %}
, closed_versions as (
    select
        c.customer_sk,
        c.customer_id,
        c.full_name,
        c.email,
        c.city,
        c.country_code,
        c.segment,
        c.row_hash,
        c.valid_from,
        ch.updated_at as valid_to,
        cast(0 as bit) as is_current,
        c.source_updated_at
    from current_versions c
    inner join changed ch
        on ch.customer_id = c.customer_id
)

select * from new_versions
union all
select * from closed_versions
{% else %}
select * from new_versions
{% endif %}

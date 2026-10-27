{{ config(unique_key='date_key') }}

-- Calendario generado con una tabla de números (10.000 días, ~27 años).
-- Nombres en castellano explícitos para no depender del idioma de la sesión.
with digits as (
    select n from (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) as d (n)
),

numbers as (
    select a.n + b.n * 10 + c.n * 100 + d.n * 1000 as n
    from digits a
    cross join digits b
    cross join digits c
    cross join digits d
),

dates as (
    select dateadd(day, n, cast('{{ var("dim_date_start") }}' as date)) as full_date
    from numbers
    where dateadd(day, n, cast('{{ var("dim_date_start") }}' as date)) <= cast('{{ var("dim_date_end") }}' as date)
),

enriched as (
    select
        full_date,
        -- Día ISO (lunes = 1) independiente de SET DATEFIRST
        (datepart(weekday, full_date) + @@datefirst + 5) % 7 + 1 as iso_weekday
    from dates
)

select
    cast(convert(char(8), full_date, 112) as int) as date_key,
    full_date,
    cast(year(full_date) as smallint) as year_number,
    cast(datepart(quarter, full_date) as tinyint) as quarter_number,
    cast(month(full_date) as tinyint) as month_number,
    cast(case month(full_date)
        when 1 then N'enero' when 2 then N'febrero' when 3 then N'marzo'
        when 4 then N'abril' when 5 then N'mayo' when 6 then N'junio'
        when 7 then N'julio' when 8 then N'agosto' when 9 then N'septiembre'
        when 10 then N'octubre' when 11 then N'noviembre' else N'diciembre'
    end as nvarchar(12)) as month_name,
    cast(day(full_date) as tinyint) as day_of_month,
    cast(iso_weekday as tinyint) as day_of_week,
    cast(case iso_weekday
        when 1 then N'lunes' when 2 then N'martes' when 3 then N'miércoles'
        when 4 then N'jueves' when 5 then N'viernes' when 6 then N'sábado' else N'domingo'
    end as nvarchar(12)) as day_name,
    cast(datepart(iso_week, full_date) as tinyint) as iso_week,
    cast(case when iso_weekday in (6, 7) then 1 else 0 end as bit) as is_weekend
from enriched
{% if is_incremental() %}
where cast(convert(char(8), full_date, 112) as int) not in (select date_key from {{ this }})
{% endif %}

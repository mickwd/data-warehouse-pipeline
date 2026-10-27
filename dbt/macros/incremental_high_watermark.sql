{#
  Máximo valor ya cargado de una columna del modelo actual.
  coalesce evita que una tabla pre-creada vacía filtre todas las filas
  (max() de un set vacío es NULL y cualquier comparación da falso).
#}
{% macro incremental_high_watermark(column, lookback_hours=0) -%}
    (select dateadd(hour, -{{ lookback_hours }}, coalesce(max({{ column }}), cast('1900-01-01' as datetime2(6)))) from {{ this }})
{%- endmacro %}

{#
  Surrogate key determinística: MD5 hex en minúsculas (CHAR(32)).
  Los nulos se reemplazan por un marcador para que (NULL, 'a') y ('', 'a')
  no colisionen.
#}
{% macro surrogate_key(columns) -%}
    cast(lower(convert(varchar(32), hashbytes('MD5', concat_ws('|'
    {%- for column in columns %},
        coalesce(cast({{ column }} as nvarchar(4000)), N'_dbt_null_')
    {%- endfor %}
    )), 2)) as char(32))
{%- endmacro %}

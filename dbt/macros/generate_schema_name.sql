{#
  Por defecto dbt concatena target.schema + schema custom (ej. dw_staging).
  Acá se usa el nombre custom tal cual para respetar los esquemas del DDL.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

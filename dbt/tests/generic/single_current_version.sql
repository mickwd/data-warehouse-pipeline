{# SCD2: cada clave natural debe tener exactamente una versión vigente. #}
{% test single_current_version(model, column_name, current_flag='is_current') %}
select {{ column_name }}, sum(cast({{ current_flag }} as int)) as current_versions
from {{ model }}
group by {{ column_name }}
having sum(cast({{ current_flag }} as int)) <> 1
{% endtest %}

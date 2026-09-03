{#-
  Asserts a set of columns is unique together. Written locally rather than
  pulled from dbt_utils so the whole suite runs offline, which keeps CI hermetic
  and lets the local stack be a faithful rehearsal of production.
-#}
{% test fs_unique_combination(model, combination_of_columns) %}

{%- set cols = combination_of_columns | join(", ") -%}

select {{ cols }}, count(*) as n_rows
from {{ model }}
group by {{ cols }}
having count(*) > 1

{% endtest %}

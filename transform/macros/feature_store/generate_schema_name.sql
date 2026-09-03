{#-
  Use custom schema names verbatim rather than dbt's default of prefixing them
  with the target schema. The feature store has a fixed layer layout
  (bronze_events / staging / intermediate / marts) and the same names
  should hold in every environment; the environment is already distinguished by
  the database or catalog.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

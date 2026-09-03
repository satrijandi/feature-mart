{#-
  Incremental strategies differ by adapter. Centralising the choice here keeps
  the generated models free of adapter conditionals.

  fs_upsert_strategy           row-level upsert keyed on the entity
                               (used by the all_time state table)
  fs_partition_replace_strategy  atomic replacement of one target_date
                               partition (used by the mart and the partial layer)
-#}

{% macro fs_upsert_strategy() %}
  {{ return({'databricks': 'merge', 'spark': 'merge', 'snowflake': 'merge'}
            .get(target.type, 'delete+insert')) }}
{% endmacro %}

{% macro fs_partition_replace_strategy() %}
  {{ return({'databricks': 'insert_overwrite', 'spark': 'insert_overwrite'}
            .get(target.type, 'delete+insert')) }}
{% endmacro %}

{#- Physical layout hints, applied only where the adapter understands them. -#}
{% macro fs_partition_config(cols) %}
  {%- if target.type in ('databricks', 'spark') -%}
    {{ return(cols) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}

{% macro fs_cluster_config(cols) %}
  {%- if target.type == 'snowflake' -%}
    {{ return(cols) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}

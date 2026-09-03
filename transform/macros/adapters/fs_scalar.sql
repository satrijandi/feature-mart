{#-
  ============================================================================
  Scalar primitives.

  fs_hash_unit(expr)        SCALAR  stable hash of expr into a DOUBLE in [0,1)
  fs_datediff_day(s, e)     SCALAR  whole CALENDAR days from s to e
                                    (both truncated to DATE first, so the
                                    result is exactly e_date - s_date)

  fs_least2 / fs_greatest2 are pure ANSI and deliberately NOT dispatched:
  LEAST/GREATEST disagree on NULL handling across engines, and the all_time
  state merge depends on "NULL means no data yet", not "NULL poisons".
  ============================================================================
-#}

{#- Largest prime below 2^53, so the quotient stays exactly representable
    as a DOUBLE on every engine. -#}
{% macro fs_hash_mod() %}9007199254740881{% endmacro %}

{% macro fs_hash_unit(expr) -%}
  {{ return(adapter.dispatch('fs_hash_unit', 'feature_mart')(expr)) }}
{%- endmacro %}

{% macro default__fs_hash_unit(expr) -%}
((hash(cast({{ expr }} as varchar)) % {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}

{% macro databricks__fs_hash_unit(expr) -%}
(pmod(xxhash64(cast({{ expr }} as string)), {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}

{% macro snowflake__fs_hash_unit(expr) -%}
(mod(abs(hash(cast({{ expr }} as varchar))), {{ fs_hash_mod() }}) / {{ fs_hash_mod() }}.0)
{%- endmacro %}


{# ------------------------------------------------------------ datediff_day #}
{% macro fs_datediff_day(start_expr, end_expr) -%}
  {{ return(adapter.dispatch('fs_datediff_day', 'feature_mart')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro default__fs_datediff_day(start_expr, end_expr) -%}
date_diff('day', cast({{ start_expr }} as date), cast({{ end_expr }} as date))
{%- endmacro %}

{#- Databricks 2-arg form is datediff(end, start) and is available on every
    runtime, unlike the 3-arg unit form. -#}
{% macro databricks__fs_datediff_day(start_expr, end_expr) -%}
datediff(cast({{ end_expr }} as date), cast({{ start_expr }} as date))
{%- endmacro %}

{% macro snowflake__fs_datediff_day(start_expr, end_expr) -%}
datediff(day, cast({{ start_expr }} as date), cast({{ end_expr }} as date))
{%- endmacro %}


{# ------------------------------------------------- null-safe min/max merges #}
{% macro fs_least2(a, b) -%}
(case
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} <= {{ b }} then {{ a }}
    else {{ b }}
 end)
{%- endmacro %}

{% macro fs_greatest2(a, b) -%}
(case
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} >= {{ b }} then {{ a }}
    else {{ b }}
 end)
{%- endmacro %}


{# ------------------------------------------------------------- target_date #}
{#- Single choke point for reading the run's as-of date. Fails loudly rather
    than silently defaulting, because a wrong target_date silently produces
    plausible-looking but time-leaking features. -#}
{% macro fs_target_date() -%}
cast('{{ fs_target_date_obj().isoformat() }}' as date)
{%- endmacro %}


{# ------------------------------------------------- as-of date arithmetic #}
{#- Window bounds are resolved to literal dates at COMPILE time rather than
    computed in SQL. Two reasons: the generated models stay free of dialect
    date-arithmetic, and Databricks/Snowflake can prune partitions against a
    literal where they cannot against an expression. -#}

{% macro fs_target_date_obj() %}
  {%- set td = var('target_date', '') -%}
  {%- if td is none or td | string | trim == '' -%}
    {{ exceptions.raise_compiler_error(
         "var 'target_date' is required. Run with: dbt run --vars '{target_date: YYYY-MM-DD}'") }}
  {%- endif -%}
  {%- if not modules.re.match('^\\d{4}-\\d{2}-\\d{2}$', td | string | trim) -%}
    {{ exceptions.raise_compiler_error(
         "var 'target_date' must be an ISO date (YYYY-MM-DD), got '" ~ td ~ "'") }}
  {%- endif -%}
  {{ return(modules.datetime.date.fromisoformat(td | string | trim)) }}
{% endmacro %}

{#- Literal DATE for `days_back` days before the as-of date. -#}
{% macro fs_date_offset_lit(days_back) -%}
  {%- set d = fs_target_date_obj() - modules.datetime.timedelta(days=days_back | int) -%}
cast('{{ d.isoformat() }}' as date)
{%- endmacro %}


{#- Bare ISO date string for the as-of date. Used to substitute the
    `{{ target_date }}` placeholder inside a user-authored source query, which
    supplies its own quoting and cast. -#}
{% macro fs_target_date_str() -%}
{{ fs_target_date_obj().isoformat() }}
{%- endmacro %}

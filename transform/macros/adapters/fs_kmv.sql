{#-
  ============================================================================
  KMV (k-Minimum-Values) sketch -- the portable `distinct_method: approx` path.

  WHY NOT NATIVE HLL: Databricks (hll_sketch_agg) and Snowflake
  (HLL_ACCUMULATE) both ship mergeable HLL, but DuckDB does not expose a
  mergeable sketch state at all, and the three binary formats are mutually
  unreadable. That would mean the local/CI stack could not reproduce the
  numbers production emits, which defeats the point of having a local stack.

  KMV needs only: a hash, array sort, array union, array slice. All three
  engines have those, so ONE implementation runs everywhere and the local
  DuckDB run is a faithful rehearsal of production.

  REPRESENTATION: sorted ascending ARRAY<DOUBLE> of the k smallest hash-units
  of the distinct input values. Fully mergeable: union two sketches, re-sort,
  keep the k smallest.

  ERROR: standard relative error is ~1/sqrt(k). k=256 -> ~6%, k=1024 -> ~3%.
  Below k distinct values the sketch holds them all and the estimate is EXACT.
  ============================================================================
-#}

{%- macro fs_kmv_k() -%}{{ var('fs_kmv_k', 256) }}{%- endmacro -%}


{#- Aggregate: build a sketch from a value expression.
    The null guard matters: DuckDB's hash(NULL) returns a real number rather
    than NULL, so an unguarded hash would silently count non-matching rows. -#}
{% macro fs_kmv_build(expr, k=none) -%}
  {%- set k = k or fs_kmv_k() -%}
  {%- set unit -%}
    (case when {{ expr }} is null then null else {{ fs_hash_unit(expr) }} end)
  {%- endset -%}
  {{ fs_array_head(fs_array_sort(fs_collect_set(unit)), k) }}
{%- endmacro %}


{#- Aggregate: union many sketches across rows (the window roll-up). -#}
{% macro fs_kmv_union_agg(col, k=none) -%}
  {%- set k = k or fs_kmv_k() -%}
  {{ fs_array_head(fs_array_sort(fs_array_union_agg(col)), k) }}
{%- endmacro %}


{#- Scalar: merge exactly two sketches (the all_time state merge). -#}
{% macro fs_kmv_merge2(a, b, k=none) -%}
  {%- set k = k or fs_kmv_k() -%}
  {{ fs_array_head(fs_array_sort(fs_array_union2(a, b)), k) }}
{%- endmacro %}


{#- Scalar: sketch -> estimated distinct count.
    Under k values the sketch is complete, so report the exact size.
    At or above k, the KMV estimator is (k-1) / m_k. -#}
{% macro fs_kmv_estimate(sketch, k=none) -%}
  {%- set k = k or fs_kmv_k() -%}
  {%- set size = fs_array_size(sketch) -%}
  (case
      when {{ size }} < {{ k }} then cast({{ size }} as bigint)
      else coalesce(
             cast(({{ k }} - 1) / nullif({{ fs_array_elem(sketch, k) }}, 0) as bigint),
             cast({{ size }} as bigint))
   end)
{%- endmacro %}

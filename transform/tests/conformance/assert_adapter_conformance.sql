-- ============================================================================
-- ADAPTER CONFORMANCE SUITE
--
-- Every dialect primitive in macros/adapters/ has a written contract. This
-- asserts each one against known inputs on WHICHEVER adapter is configured, so
-- `dbt test --target databricks` proves the Databricks implementations agree
-- with the DuckDB ones rather than merely compiling.
--
-- Porting the feature store to a new warehouse means: implement the macros,
-- run this, ship. If this passes, every generated model is portable, because
-- generated models use nothing else.
--
-- Each violated contract names itself, so a failure identifies the primitive.
-- ============================================================================

with

vals as (
    -- 'a' twice (dedup) and a NULL (must be dropped by collect_set)
    select * from (values (1, 'a'), (1, 'b'), (1, 'a'), (1, cast(null as varchar))) as t(g, v)
),

other_vals as (
    select * from (values (1, 'b'), (1, 'c')) as t(g, v)
),

all_null as (
    select * from (values (1, cast(null as varchar))) as t(g, v)
),

set_a   as (select g, {{ fs_collect_set('v') }} as s from vals       group by g),
set_b   as (select g, {{ fs_collect_set('v') }} as s from other_vals group by g),
set_nul as (select g, {{ fs_collect_set('v') }} as s from all_null   group by g),

-- A genuinely NULL array of the right element type, obtained the way the
-- pipeline obtains one: a left join that does not match.
with_null_array as (
    select a.g, a.s as s, missing.s as null_s
    from set_a a
    left join (select * from set_b where 1 = 0) as missing on a.g = missing.g
),

-- Union of set_a and set_b across ROWS, exercising the aggregate form.
unioned as (
    select g, {{ fs_array_union_agg('s') }} as s
    from (select * from set_a union all select * from set_b) as u
    group by g
),

-- KMV round trip: build, union across rows, merge two sketches, estimate.
-- Below k a sketch retains every hash, so all three must be exact.
sketch_a as (select g, {{ fs_kmv_build('v', 64) }} as k from vals       group by g),
sketch_b as (select g, {{ fs_kmv_build('v', 64) }} as k from other_vals group by g),
sketch_union as (
    select g, {{ fs_kmv_union_agg('k', 64) }} as k
    from (select * from sketch_a union all select * from sketch_b) as u
    group by g
),

-- A NULL sketch of the correct element type (array<double>, not array<varchar>),
-- which is what the mart sees for an entity absent from the unsealed tail.
with_null_sketch as (
    select sa.g, missing.k as null_k
    from sketch_a sa
    left join (select * from sketch_b where 1 = 0) as missing on sa.g = missing.g
),

checked as (

    select
        coalesce(
            case when {{ fs_array_size('a.s') }} <> 2
                 then 'fs_collect_set:dedups_and_drops_nulls;' end, '') ||
        coalesce(
            case when {{ fs_array_size('n.s') }} <> 0
                 then 'fs_collect_set:all_null_group_is_empty;' end, '') ||
        coalesce(
            case when {{ fs_array_size('w.null_s') }} <> 0
                 then 'fs_array_size:null_is_zero;' end, '') ||
        coalesce(
            case when {{ fs_array_size('u.s') }} <> 3
                 then 'fs_array_union_agg:unions_across_rows;' end, '') ||
        coalesce(
            case when {{ fs_array_size(fs_array_union2('a.s', 'b.s')) }} <> 3
                 then 'fs_array_union2:dedups;' end, '') ||
        coalesce(
            case when {{ fs_array_size(fs_array_union2('a.s', 'w.null_s')) }} <> 2
                 then 'fs_array_union2:null_safe;' end, '') ||
        coalesce(
            case when {{ fs_array_elem(fs_array_sort('a.s'), 1) }} <> 'a'
                 then 'fs_array_sort:ascending;' end, '') ||
        coalesce(
            case when {{ fs_array_elem(fs_array_sort('a.s'), 2) }} <> 'b'
                 then 'fs_array_elem:is_one_based;' end, '') ||
        coalesce(
            case when {{ fs_array_elem(fs_array_sort('a.s'), 9) }} is not null
                 then 'fs_array_elem:out_of_range_is_null;' end, '') ||
        coalesce(
            case when {{ fs_array_size(fs_array_head(fs_array_sort('a.s'), 1)) }} <> 1
                 then 'fs_array_head:takes_n;' end, '') ||
        coalesce(
            case when {{ fs_array_size(fs_array_head(fs_array_sort('a.s'), 99)) }} <> 2
                 then 'fs_array_head:tolerates_short_arrays;' end, '') ||

        coalesce(
            case when {{ fs_hash_unit("'x'") }} < 0 or {{ fs_hash_unit("'x'") }} >= 1
                 then 'fs_hash_unit:in_unit_interval;' end, '') ||
        coalesce(
            case when {{ fs_hash_unit("'x'") }} <> {{ fs_hash_unit("'x'") }}
                 then 'fs_hash_unit:deterministic;' end, '') ||
        coalesce(
            case when {{ fs_hash_unit("'x'") }} = {{ fs_hash_unit("'y'") }}
                 then 'fs_hash_unit:distinguishes_inputs;' end, '') ||

        coalesce(
            case when {{ fs_datediff_day("cast('2026-01-01' as date)", "cast('2026-01-10' as date)") }} <> 9
                 then 'fs_datediff_day:counts_calendar_days;' end, '') ||
        coalesce(
            case when {{ fs_datediff_day("cast('2026-01-01 23:59:00' as timestamp)", "cast('2026-01-02 00:01:00' as timestamp)") }} <> 1
                 then 'fs_datediff_day:truncates_to_date;' end, '') ||
        coalesce(
            case when {{ fs_datediff_day("cast('2026-01-10' as date)", "cast('2026-01-01' as date)") }} <> -9
                 then 'fs_datediff_day:signed;' end, '') ||

        coalesce(
            case when {{ fs_least2('cast(null as bigint)', 'cast(5 as bigint)') }} <> 5
                 then 'fs_least2:null_is_absorbing_not_poisoning;' end, '') ||
        coalesce(
            case when {{ fs_least2('cast(3 as bigint)', 'cast(5 as bigint)') }} <> 3
                 then 'fs_least2:picks_smaller;' end, '') ||
        coalesce(
            case when {{ fs_greatest2('cast(null as bigint)', 'cast(5 as bigint)') }} <> 5
                 then 'fs_greatest2:null_is_absorbing;' end, '') ||
        coalesce(
            case when {{ fs_greatest2('cast(3 as bigint)', 'cast(5 as bigint)') }} <> 5
                 then 'fs_greatest2:picks_larger;' end, '') ||

        coalesce(
            case when {{ fs_kmv_estimate('sa.k', 64) }} <> 2
                 then 'fs_kmv_build:exact_below_k;' end, '') ||
        coalesce(
            case when {{ fs_kmv_estimate('su.k', 64) }} <> 3
                 then 'fs_kmv_union_agg:exact_below_k;' end, '') ||
        coalesce(
            case when {{ fs_kmv_estimate(fs_kmv_merge2('sa.k', 'sb.k', 64), 64) }} <> 3
                 then 'fs_kmv_merge2:exact_below_k;' end, '') ||
        coalesce(
            case when {{ fs_kmv_estimate('ns.null_k', 64) }} <> 0
                 then 'fs_kmv_estimate:null_sketch_is_zero;' end, '') ||
        coalesce(
            case when {{ fs_kmv_estimate(fs_kmv_merge2('sa.k', 'ns.null_k', 64), 64) }} <> 2
                 then 'fs_kmv_merge2:null_safe;' end, '') ||
        '' as violations

    from set_a a
    join set_b b on a.g = b.g
    join set_nul n on a.g = n.g
    join with_null_array w on a.g = w.g
    join unioned u on a.g = u.g
    join sketch_a sa on a.g = sa.g
    join sketch_b sb on a.g = sb.g
    join sketch_union su on a.g = su.g
    join with_null_sketch ns on a.g = ns.g

)

select '{{ target.type }}' as adapter, violations
from checked
where violations <> ''

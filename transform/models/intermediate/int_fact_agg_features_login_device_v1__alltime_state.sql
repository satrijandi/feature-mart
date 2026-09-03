-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / all_time sealed state
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : 7e3c413228a7
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- The running all_time accumulator: one row per entity, holding the same
-- composable state as a daily partial but folded over all sealed history.
--
-- SEALING. Only event_dates at or before target_date - 3 are folded in,
-- because more recent days may still receive late arrivals and get rewritten.
-- The mart completes all_time by merging this with the unsealed tail, using
-- the same merge function, so the published number is never stale.
--
-- SELF-HEALING AND IDEMPOTENT. The consumed range is
-- (stored watermark, seal date], not "yesterday". A retried run consumes an
-- empty range and changes nothing; a run that follows a missed day picks the
-- gap up automatically. Neither case needs an operator.
--
-- Only entities with activity in the range are written. An entity with no
-- events in the range has, by construction, nothing to fold, so leaving its
-- row untouched is correct and keeps the write volume proportional to
-- activity rather than to population.
--
-- A consequence worth knowing: across a range with NO activity at all,
-- nothing is written and the watermark does not advance. That is accurate
-- rather than stuck -- the state really is sealed only through the old
-- watermark -- and the next run with data simply consumes the wider range.
-- It does mean the watermark tracks the last day with events, not the last
-- day attempted.

{{ config(
    materialized='incremental',
    incremental_strategy=fs_upsert_strategy(),
    unique_key=['safe_id'],
    on_schema_change='sync_all_columns',
    tags=['feature_store', 'fact_agg_features_login_device_v1']
) }}

with watermark as (

    {% if is_incremental() %}
    select coalesce(max(_state_as_of_date), cast('1900-01-01' as date)) as wm
    from {{ this }}
    {% else %}
    select cast('1900-01-01' as date) as wm
    {% endif %}

),

new_days as (

    select
        p.safe_id,
        -- event_id / count ----------------------------------------------------
        sum(p.p_count_event_id) as p_count_event_id,
        sum(p.p_count_event_id_is_login_success) as p_count_event_id_is_login_success,
        sum(p.p_count_event_id_is_login_failed) as p_count_event_id_is_login_failed,

        -- event_id / count_distinct -------------------------------------------
        {{ fs_kmv_union_agg("p.p_count_distinct_event_id", 64) }} as p_count_distinct_event_id,
        {{ fs_kmv_union_agg("p.p_count_distinct_event_id_is_login_success", 64) }} as p_count_distinct_event_id_is_login_success,
        {{ fs_kmv_union_agg("p.p_count_distinct_event_id_is_login_failed", 64) }} as p_count_distinct_event_id_is_login_failed,

        -- device_id / count_distinct ------------------------------------------
        {{ fs_array_union_agg("p.p_count_distinct_device_id") }} as p_count_distinct_device_id,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_success") }} as p_count_distinct_device_id_is_login_success,
        {{ fs_array_union_agg("p.p_count_distinct_device_id_is_login_failed") }} as p_count_distinct_device_id_is_login_failed,

        -- login_source / count_distinct ---------------------------------------
        {{ fs_array_union_agg("p.p_count_distinct_login_source") }} as p_count_distinct_login_source,
        {{ fs_array_union_agg("p.p_count_distinct_login_source_is_login_success") }} as p_count_distinct_login_source_is_login_success,
        {{ fs_array_union_agg("p.p_count_distinct_login_source_is_login_failed") }} as p_count_distinct_login_source_is_login_failed,

        min(p.event_date) as _min_event_date,
        max(p.event_date) as _max_event_date
    from {{ ref('int_fact_agg_features_login_device_v1__daily_partials') }} p
    cross join watermark w
    where p.event_date > w.wm
      and p.event_date <= {{ fs_date_offset_lit(3) }}
    group by p.safe_id

),

prev as (

    {% if is_incremental() %}
    select * from {{ this }}
    {% else %}
    -- First build: same shape, no rows, so the merge below is the only
    -- projection in the model and cannot diverge between branches.
    select * from new_days where 1 = 0
    {% endif %}

)

select
    n.safe_id,
    -- event_id / count ----------------------------------------------------
    (coalesce(prev.p_count_event_id, 0) + coalesce(n.p_count_event_id, 0)) as p_count_event_id,
    (coalesce(prev.p_count_event_id_is_login_success, 0) + coalesce(n.p_count_event_id_is_login_success, 0)) as p_count_event_id_is_login_success,
    (coalesce(prev.p_count_event_id_is_login_failed, 0) + coalesce(n.p_count_event_id_is_login_failed, 0)) as p_count_event_id_is_login_failed,

    -- event_id / count_distinct -------------------------------------------
    {{ fs_kmv_merge2("prev.p_count_distinct_event_id", "n.p_count_distinct_event_id", 64) }} as p_count_distinct_event_id,
    {{ fs_kmv_merge2("prev.p_count_distinct_event_id_is_login_success", "n.p_count_distinct_event_id_is_login_success", 64) }} as p_count_distinct_event_id_is_login_success,
    {{ fs_kmv_merge2("prev.p_count_distinct_event_id_is_login_failed", "n.p_count_distinct_event_id_is_login_failed", 64) }} as p_count_distinct_event_id_is_login_failed,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_array_union2("prev.p_count_distinct_device_id", "n.p_count_distinct_device_id") }} as p_count_distinct_device_id,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_success", "n.p_count_distinct_device_id_is_login_success") }} as p_count_distinct_device_id_is_login_success,
    {{ fs_array_union2("prev.p_count_distinct_device_id_is_login_failed", "n.p_count_distinct_device_id_is_login_failed") }} as p_count_distinct_device_id_is_login_failed,

    -- login_source / count_distinct ---------------------------------------
    {{ fs_array_union2("prev.p_count_distinct_login_source", "n.p_count_distinct_login_source") }} as p_count_distinct_login_source,
    {{ fs_array_union2("prev.p_count_distinct_login_source_is_login_success", "n.p_count_distinct_login_source_is_login_success") }} as p_count_distinct_login_source_is_login_success,
    {{ fs_array_union2("prev.p_count_distinct_login_source_is_login_failed", "n.p_count_distinct_login_source_is_login_failed") }} as p_count_distinct_login_source_is_login_failed,

    {{ fs_least2('prev._min_event_date', 'n._min_event_date') }} as _min_event_date,
    {{ fs_greatest2('prev._max_event_date', 'n._max_event_date') }} as _max_event_date,
    {{ fs_date_offset_lit(3) }} as _state_as_of_date,
    '7e3c413228a7' as _spec_version
from new_days n
left join prev on n.safe_id = prev.safe_id

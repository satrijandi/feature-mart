-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : mart / published feature table
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : aaec8c5d5bcb
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- One row per entity per as-of date, 36 feature columns wide.
--
-- POINT-IN-TIME SAFETY. Nothing here can see past target_date: the partial
-- layer refuses events after it and every window is bounded above by it, so
-- joining this table to labels on target_date is leak-free by construction.

{{ config(
    materialized='incremental',
    incremental_strategy=fs_partition_replace_strategy(),
    unique_key=['target_date'],
    partition_by=fs_partition_config(['target_date']),
    cluster_by=fs_cluster_config(['target_date']),
    on_schema_change='sync_all_columns',
    tags=['feature_store', 'fact_agg_features_login_device_v1', 'mart']
) }}

with

rollup as (

    select * from {{ ref('int_fact_agg_features_login_device_v1__window_rollup') }}

),

at_state as (

    select * from {{ ref('int_fact_agg_features_login_device_v1__alltime_state') }}

),

at_recent as (

    select * from {{ ref('int_fact_agg_features_login_device_v1__alltime_recent') }}

),

spine as (

    -- entity_spine: all_time. Every entity ever seen gets a row on every
    -- as-of date, so a training-set join never silently drops a dormant
    -- population. This is the expensive option by design; switch to
    -- active_window in the spec if the daily row count outgrows its value.
    select safe_id from rollup
    union
    select safe_id from at_state

),

joined as (

    select
        spine.safe_id,

        -- event_id / count ----------------------------------------------------
        coalesce(rollup.count_event_id_l7d, 0) as count_event_id_l7d,
        coalesce(rollup.count_event_id_l30d, 0) as count_event_id_l30d,
        coalesce((coalesce(at_state.p_count_event_id, 0) + coalesce(at_recent.p_count_event_id, 0)), 0) as count_event_id_all_time,
        coalesce(rollup.count_event_id_is_login_success_l7d, 0) as count_event_id_is_login_success_l7d,
        coalesce(rollup.count_event_id_is_login_success_l30d, 0) as count_event_id_is_login_success_l30d,
        coalesce((coalesce(at_state.p_count_event_id_is_login_success, 0) + coalesce(at_recent.p_count_event_id_is_login_success, 0)), 0) as count_event_id_is_login_success_all_time,
        coalesce(rollup.count_event_id_is_login_failed_l7d, 0) as count_event_id_is_login_failed_l7d,
        coalesce(rollup.count_event_id_is_login_failed_l30d, 0) as count_event_id_is_login_failed_l30d,
        coalesce((coalesce(at_state.p_count_event_id_is_login_failed, 0) + coalesce(at_recent.p_count_event_id_is_login_failed, 0)), 0) as count_event_id_is_login_failed_all_time,

        -- event_id / count_distinct -------------------------------------------
        coalesce(rollup.count_distinct_event_id_l7d, 0) as count_distinct_event_id_l7d,
        coalesce(rollup.count_distinct_event_id_l30d, 0) as count_distinct_event_id_l30d,
        {{ fs_kmv_estimate(fs_kmv_merge2("at_state.p_count_distinct_event_id", "at_recent.p_count_distinct_event_id", 64), 64) }} as count_distinct_event_id_all_time,
        coalesce(rollup.count_distinct_event_id_is_login_success_l7d, 0) as count_distinct_event_id_is_login_success_l7d,
        coalesce(rollup.count_distinct_event_id_is_login_success_l30d, 0) as count_distinct_event_id_is_login_success_l30d,
        {{ fs_kmv_estimate(fs_kmv_merge2("at_state.p_count_distinct_event_id_is_login_success", "at_recent.p_count_distinct_event_id_is_login_success", 64), 64) }} as count_distinct_event_id_is_login_success_all_time,
        coalesce(rollup.count_distinct_event_id_is_login_failed_l7d, 0) as count_distinct_event_id_is_login_failed_l7d,
        coalesce(rollup.count_distinct_event_id_is_login_failed_l30d, 0) as count_distinct_event_id_is_login_failed_l30d,
        {{ fs_kmv_estimate(fs_kmv_merge2("at_state.p_count_distinct_event_id_is_login_failed", "at_recent.p_count_distinct_event_id_is_login_failed", 64), 64) }} as count_distinct_event_id_is_login_failed_all_time,

        -- device_id / count_distinct ------------------------------------------
        coalesce(rollup.count_distinct_device_id_l7d, 0) as count_distinct_device_id_l7d,
        coalesce(rollup.count_distinct_device_id_l30d, 0) as count_distinct_device_id_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_device_id", "at_recent.p_count_distinct_device_id")) }} as count_distinct_device_id_all_time,
        coalesce(rollup.count_distinct_device_id_is_login_success_l7d, 0) as count_distinct_device_id_is_login_success_l7d,
        coalesce(rollup.count_distinct_device_id_is_login_success_l30d, 0) as count_distinct_device_id_is_login_success_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_device_id_is_login_success", "at_recent.p_count_distinct_device_id_is_login_success")) }} as count_distinct_device_id_is_login_success_all_time,
        coalesce(rollup.count_distinct_device_id_is_login_failed_l7d, 0) as count_distinct_device_id_is_login_failed_l7d,
        coalesce(rollup.count_distinct_device_id_is_login_failed_l30d, 0) as count_distinct_device_id_is_login_failed_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_device_id_is_login_failed", "at_recent.p_count_distinct_device_id_is_login_failed")) }} as count_distinct_device_id_is_login_failed_all_time,

        -- login_source / count_distinct ---------------------------------------
        coalesce(rollup.count_distinct_login_source_l7d, 0) as count_distinct_login_source_l7d,
        coalesce(rollup.count_distinct_login_source_l30d, 0) as count_distinct_login_source_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_login_source", "at_recent.p_count_distinct_login_source")) }} as count_distinct_login_source_all_time,
        coalesce(rollup.count_distinct_login_source_is_login_success_l7d, 0) as count_distinct_login_source_is_login_success_l7d,
        coalesce(rollup.count_distinct_login_source_is_login_success_l30d, 0) as count_distinct_login_source_is_login_success_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_login_source_is_login_success", "at_recent.p_count_distinct_login_source_is_login_success")) }} as count_distinct_login_source_is_login_success_all_time,
        coalesce(rollup.count_distinct_login_source_is_login_failed_l7d, 0) as count_distinct_login_source_is_login_failed_l7d,
        coalesce(rollup.count_distinct_login_source_is_login_failed_l30d, 0) as count_distinct_login_source_is_login_failed_l30d,
        {{ fs_array_size(fs_array_union2("at_state.p_count_distinct_login_source_is_login_failed", "at_recent.p_count_distinct_login_source_is_login_failed")) }} as count_distinct_login_source_is_login_failed_all_time,

        {{ fs_least2('at_state._min_event_date', 'at_recent._min_event_date') }} as _first_event_date,
        {{ fs_greatest2('at_state._max_event_date', 'at_recent._max_event_date') }} as _last_event_date
    from spine
    left join rollup on spine.safe_id = rollup.safe_id
    left join at_state on spine.safe_id = at_state.safe_id
    left join at_recent on spine.safe_id = at_recent.safe_id

)

select
    {{ fs_target_date() }} as target_date,
    safe_id,

    -- event_id / count ----------------------------------------------------
    count_event_id_l7d,
    count_event_id_l30d,
    count_event_id_all_time,
    count_event_id_is_login_success_l7d,
    count_event_id_is_login_success_l30d,
    count_event_id_is_login_success_all_time,
    count_event_id_is_login_failed_l7d,
    count_event_id_is_login_failed_l30d,
    count_event_id_is_login_failed_all_time,

    -- event_id / count_distinct -------------------------------------------
    count_distinct_event_id_l7d,
    count_distinct_event_id_l30d,
    count_distinct_event_id_all_time,
    count_distinct_event_id_is_login_success_l7d,
    count_distinct_event_id_is_login_success_l30d,
    count_distinct_event_id_is_login_success_all_time,
    count_distinct_event_id_is_login_failed_l7d,
    count_distinct_event_id_is_login_failed_l30d,
    count_distinct_event_id_is_login_failed_all_time,

    -- device_id / count_distinct ------------------------------------------
    count_distinct_device_id_l7d,
    count_distinct_device_id_l30d,
    count_distinct_device_id_all_time,
    count_distinct_device_id_is_login_success_l7d,
    count_distinct_device_id_is_login_success_l30d,
    count_distinct_device_id_is_login_success_all_time,
    count_distinct_device_id_is_login_failed_l7d,
    count_distinct_device_id_is_login_failed_l30d,
    count_distinct_device_id_is_login_failed_all_time,

    -- login_source / count_distinct ---------------------------------------
    count_distinct_login_source_l7d,
    count_distinct_login_source_l30d,
    count_distinct_login_source_all_time,
    count_distinct_login_source_is_login_success_l7d,
    count_distinct_login_source_is_login_success_l30d,
    count_distinct_login_source_is_login_success_all_time,
    count_distinct_login_source_is_login_failed_l7d,
    count_distinct_login_source_is_login_failed_l30d,
    count_distinct_login_source_is_login_failed_all_time,

    _first_event_date,
    _last_event_date,
    {{ dbt.current_timestamp() }} as _generated_at,
    'aaec8c5d5bcb' as _spec_version
from joined

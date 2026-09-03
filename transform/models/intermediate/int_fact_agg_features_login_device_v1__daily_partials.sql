-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : intermediate / daily partial aggregates
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : 01ce520f1167
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- One row per entity per event_date holding COMPOSABLE state, not finished
-- features. Every window and all_time is a fold over these rows, so a day is
-- read from the source exactly once no matter how many windows consume it.
--
-- Each run rewrites the last 3 day(s) as well as today, so events that
-- arrive late still land in the day they belong to. That is why the all_time
-- accumulator seals only up to target_date - 3.

{{ config(
    materialized='incremental',
    incremental_strategy=fs_partition_replace_strategy(),
    unique_key=['safe_id', 'event_date'],
    partition_by=fs_partition_config(['event_date']),
    cluster_by=fs_cluster_config(['event_date']),
    on_schema_change='sync_all_columns',
    tags=['feature_store', 'fact_agg_features_login_device_v1']
) }}

with

{% if is_incremental() %}
rewrite_window as (

    -- The rewrite window is anchored to the last as-of date ACTUALLY
    -- processed, not to this run's. If runs were missed, the days that were
    -- still inside their late-arrival window back then are still incomplete
    -- now, and a window measured from today would step straight over them --
    -- losing those events permanently, with nothing to signal it. Taking the
    -- earlier of the two anchors makes the partial layer self-heal across a
    -- gap, exactly as the all_time accumulator does.
    select coalesce(max(_computed_for), cast('1900-01-01' as date)) as last_run
    from {{ this }}

),

-- MONOTONICITY GUARD.
-- Each run sees the source as of ITS OWN target_date, so recomputing an
-- event_date under an earlier as-of date would see fewer rows than a later
-- run already stored and silently drop events. Runs are not guaranteed to
-- arrive in order -- backfills, manual replays and retried tasks all break
-- that assumption -- so an event_date already computed under a LATER as-of
-- date is left alone. Stored partials can therefore only ever gain
-- information, never lose it.
already_fresher as (

    select distinct event_date
    from {{ this }}
    where _computed_for > {{ fs_date_offset_lit(0) }}

),
{% endif %}

events as (

    select s.*
    from {{ ref('stg_fact_agg_features_login_device_v1') }} s
    {% if is_incremental() %}
    cross join rewrite_window w
    {% endif %}
    where s.event_date <= {{ fs_date_offset_lit(0) }}
    {% if var('fs_backfill_from', none) %}
      -- Initial load / replay: widen the window to the requested start date.
      and s.event_date >= cast('{{ var('fs_backfill_from') }}' as date)
    {% elif is_incremental() %}
      and {{ fs_datediff_day('s.event_date',
                            fs_least2('w.last_run', fs_target_date())) }} <= 3
      and s.event_date not in (select event_date from already_fresher)
    {% else %}
      and s.event_date >= {{ fs_date_offset_lit(3) }}
    {% endif %}

)

select
    safe_id,
    event_date,
    -- event_id / count ----------------------------------------------------
    count(event_id) as p_count_event_id,
    count(case when (UPPER(event_status) = 'SUCCESS') then event_id end) as p_count_event_id_is_login_success,
    count(case when (UPPER(event_status) = 'FAILED') then event_id end) as p_count_event_id_is_login_failed,

    -- event_id / count_distinct -------------------------------------------
    {{ fs_kmv_build("event_id", 64) }} as p_count_distinct_event_id,
    {{ fs_kmv_build("case when (UPPER(event_status) = 'SUCCESS') then event_id end", 64) }} as p_count_distinct_event_id_is_login_success,
    {{ fs_kmv_build("case when (UPPER(event_status) = 'FAILED') then event_id end", 64) }} as p_count_distinct_event_id_is_login_failed,

    -- device_id / count_distinct ------------------------------------------
    {{ fs_collect_set("cast(device_id as varchar)") }} as p_count_distinct_device_id,
    {{ fs_collect_set("cast(case when (UPPER(event_status) = 'SUCCESS') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_success,
    {{ fs_collect_set("cast(case when (UPPER(event_status) = 'FAILED') then device_id end as varchar)") }} as p_count_distinct_device_id_is_login_failed,

    -- login_source / count_distinct ---------------------------------------
    {{ fs_collect_set("cast(login_source as varchar)") }} as p_count_distinct_login_source,
    {{ fs_collect_set("cast(case when (UPPER(event_status) = 'SUCCESS') then login_source end as varchar)") }} as p_count_distinct_login_source_is_login_success,
    {{ fs_collect_set("cast(case when (UPPER(event_status) = 'FAILED') then login_source end as varchar)") }} as p_count_distinct_login_source_is_login_failed,

    {{ fs_date_offset_lit(0) }} as _computed_for,
    '01ce520f1167' as _spec_version
from events
group by safe_id, event_date

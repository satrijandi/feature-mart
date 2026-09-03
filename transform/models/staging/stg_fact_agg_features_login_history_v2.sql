-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : staging
--   feature    : fact_agg_features_login_history_v2
--   spec       : features/fact_agg_features_login_history_v2.yml
--   spec hash  : 6c9b3cc88a21
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- Binds the source query to a single as-of date and projects only the
-- columns the feature layer may legally see.

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_history_v2']) }}

select
    customer_id as safe_id,
    device_id,
    event_id,
    event_timestamp,
    login_source,
    os_name,
    event_status,
    cast(event_timestamp as date) as event_date,

    -- Removed from this projection on purpose: days_since_login
    -- Each is a function of the as-of date, so storing it in the daily
    -- partial layer would make yesterday's partials wrong today. The mart
    -- rebuilds them from stored timestamps instead, which is exact and
    -- keeps the partials reusable across every as-of date.

    {{ fs_target_date() }} as _compiled_for_date
FROM {{ source('bronze_events', 'customer_login') }}
WHERE
  ('{{ fs_target_date_str() }}'::DATE >= _scd_valid_from AND '{{ fs_target_date_str() }}'::DATE < _scd_valid_to)
  AND customer_id IS NOT NULL

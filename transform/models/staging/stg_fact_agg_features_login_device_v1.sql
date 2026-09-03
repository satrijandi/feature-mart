-- ============================================================================
-- GENERATED FILE - DO NOT EDIT BY HAND
--
--   layer      : staging
--   feature    : fact_agg_features_login_device_v1
--   spec       : features/fact_agg_features_login_device_v1.yml
--   spec hash  : 7e3c413228a7
--   generator  : featuremart
--
-- Edit the spec and run `make generate`. CI fails when a generated file
-- differs from what the spec produces, so this file and the spec cannot
-- drift apart.
-- ============================================================================

-- Binds the source query to a single as-of date and projects only the
-- columns the feature layer may legally see.

{{ config(materialized='view', tags=['feature_store', 'fact_agg_features_login_device_v1']) }}

select
    customer_id as safe_id,
    device_id,
    event_id,
    event_timestamp,
    login_source,
    os_name,
    event_status,
    cast(event_timestamp as date) as event_date,
    {{ fs_target_date() }} as _compiled_for_date
FROM {{ source('bronze_backend_ddb', 'customer_journal_login') }}
WHERE
  ('{{ fs_target_date_str() }}'::DATE >= _scd_valid_from AND '{{ fs_target_date_str() }}'::DATE < _scd_valid_to)
  AND customer_id IS NOT NULL

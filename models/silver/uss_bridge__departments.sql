MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__department)
);

SELECT
  'departments' AS peripheral,
  _hook__department::BLOB,
  department__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  department__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  department__record_version::TEXT AS bridge__record_version,
  department__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  department__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  department__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__departments
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
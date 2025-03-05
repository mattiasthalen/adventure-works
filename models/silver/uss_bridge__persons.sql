MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__individual)
);

SELECT
  'persons' AS peripheral,
  _hook__person__individual::BLOB,
  person__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  person__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  person__record_version::TEXT AS bridge__record_version,
  person__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  person__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  person__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__persons
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
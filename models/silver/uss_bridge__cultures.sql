MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__culture)
);

SELECT
  'cultures' AS peripheral,
  _hook__reference__culture::BLOB,
  culture__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  culture__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  culture__record_version::TEXT AS bridge__record_version,
  culture__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  culture__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  culture__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__cultures
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__reference__unit_measure)
);

SELECT
  'unit_measures' AS peripheral,
  _hook__reference__unit_measure::BLOB,
  unit_measure__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  unit_measure__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  unit_measure__record_version::TEXT AS bridge__record_version,
  unit_measure__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  unit_measure__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  unit_measure__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__unit_measures
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
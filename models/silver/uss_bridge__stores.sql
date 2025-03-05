MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__store),
  references (_hook__person__sales)
);

SELECT
  'stores' AS peripheral,
  _hook__store::BLOB,
  _hook__person__sales::BLOB,
  store__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  store__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  store__record_version::TEXT AS bridge__record_version,
  store__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  store__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  store__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__stores
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__currency)
);

SELECT
  'currencies' AS peripheral,
  _hook__currency::BLOB,
  currency__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  currency__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  currency__record_version::TEXT AS bridge__record_version,
  currency__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  currency__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  currency__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__currencies
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__currency_rate),
  references (_hook__currency__from, _hook__currency__to)
);

SELECT
  'currency_rates' AS peripheral,
  _hook__currency__from::BLOB,
  _hook__currency__to::BLOB,
  _hook__currency_rate::BLOB,
  currency_rate__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  currency_rate__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  currency_rate__record_version::TEXT AS bridge__record_version,
  currency_rate__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  currency_rate__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  currency_rate__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__currency_rates
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
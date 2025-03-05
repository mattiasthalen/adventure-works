MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__individual),
  references (_hook__email_address)
);

SELECT
  'email_addresses' AS peripheral,
  _hook__person__individual::BLOB,
  _hook__email_address::BLOB,
  email_address__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  email_address__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  email_address__record_version::TEXT AS bridge__record_version,
  email_address__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  email_address__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  email_address__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__email_addresses
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
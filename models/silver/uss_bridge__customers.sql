MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__customer),
  references (_hook__person__customer, _hook__store, _hook__territory__sales)
);

SELECT
  'customers' AS peripheral,
  _hook__customer::BLOB,
  _hook__person__customer::BLOB,
  _hook__store::BLOB,
  _hook__territory__sales::BLOB,
  customer__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  customer__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  customer__record_version::TEXT AS bridge__record_version,
  customer__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  customer__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  customer__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__customers
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
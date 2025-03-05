MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__person__sales),
  references (_hook__territory__sales)
);

SELECT
  'sales_persons' AS peripheral,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
  sales_person__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_person__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_person__record_version::TEXT AS bridge__record_version,
  sales_person__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_person__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_person__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_persons
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
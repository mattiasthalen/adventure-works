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
  'sales_territory_histories' AS peripheral,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
  sales_territory_history__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_territory_history__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_territory_history__record_version::TEXT AS bridge__record_version,
  sales_territory_history__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_territory_history__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_territory_history__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_territory_histories
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
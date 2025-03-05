MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__order__purchase),
  references (_hook__person__employee, _hook__vendor, _hook__ship_method)
);

SELECT
  'purchase_order_headers' AS peripheral,
  _hook__order__purchase::BLOB,
  _hook__person__employee::BLOB,
  _hook__vendor::BLOB,
  _hook__ship_method::BLOB,
  purchase_order_header__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  purchase_order_header__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  purchase_order_header__record_version::TEXT AS bridge__record_version,
  purchase_order_header__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  purchase_order_header__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  purchase_order_header__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__purchase_order_headers
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
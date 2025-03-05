MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__order__sales),
  references (_hook__customer, _hook__person__sales, _hook__territory__sales, _hook__address__billing, _hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency)
);

SELECT
  'sales_order_headers' AS peripheral,
  _hook__order__sales::BLOB,
  _hook__customer::BLOB,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
  _hook__address__billing::BLOB,
  _hook__address__shipping::BLOB,
  _hook__ship_method::BLOB,
  _hook__credit_card::BLOB,
  _hook__currency::BLOB,
  sales_order_header__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  sales_order_header__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  sales_order_header__record_version::TEXT AS bridge__record_version,
  sales_order_header__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  sales_order_header__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  sales_order_header__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_order_headers
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
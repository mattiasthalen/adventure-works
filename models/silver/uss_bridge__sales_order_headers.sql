MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (peripheral, _pit_hook__order__sales, _pit_hook__customer, _hook__person__sales, _hook__territory__sales, _pit_hook__address__billing, _pit_hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency),
  references (_pit_hook__order__sales, _pit_hook__customer, _hook__person__sales, _hook__territory__sales, _pit_hook__address__billing, _pit_hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency)
);

SELECT
  'sales_order_headers' AS peripheral,
  bag__adventure_works__sales_order_headers._pit_hook__order__sales::BLOB,
  bag__adventure_works__customers._pit_hook__customer::BLOB,
  bag__adventure_works__sales_order_headers._hook__person__sales::BLOB,
  bag__adventure_works__sales_order_headers._hook__territory__sales::BLOB,
  addresses__billing._pit_hook__address::BLOB AS _pit_hook__address__billing,
  addresses__shipping._pit_hook__address::BLOB AS _pit_hook__address__shipping,
  bag__adventure_works__sales_order_headers._hook__ship_method::BLOB,
  bag__adventure_works__sales_order_headers._hook__credit_card::BLOB,
  bag__adventure_works__sales_order_headers._hook__currency::BLOB,
  GREATEST(
    bag__adventure_works__sales_order_headers.sales_order_header__record_loaded_at,
    bag__adventure_works__customers.customer__record_loaded_at,
    addresses__billing.address__record_loaded_at,
    addresses__shipping.address__record_loaded_at,
  )::TIMESTAMP AS bridge__record_loaded_at,
  GREATEST(
    bag__adventure_works__sales_order_headers.sales_order_header__record_updated_at,
    bag__adventure_works__customers.customer__record_updated_at,
    addresses__billing.address__record_updated_at,
    addresses__shipping.address__record_updated_at,
  )::TIMESTAMP AS bridge__record_updated_at,
  GREATEST(
    bag__adventure_works__sales_order_headers.sales_order_header__record_valid_from,
    bag__adventure_works__customers.customer__record_valid_from,
    addresses__billing.address__record_valid_from,
    addresses__shipping.address__record_valid_from,
  )::TIMESTAMP AS bridge__record_valid_from,
  LEAST(
    bag__adventure_works__sales_order_headers.sales_order_header__record_valid_to,
    bag__adventure_works__customers.customer__record_valid_to,
    addresses__billing.address__record_valid_to,
    addresses__shipping.address__record_valid_to,
  )::TIMESTAMP AS bridge__record_valid_to,
  LIST_HAS_ALL(
    ARRAY[TRUE],
    ARRAY[
        bag__adventure_works__sales_order_headers.sales_order_header__is_current_record::BOOL,
        bag__adventure_works__customers.customer__is_current_record::BOOL,
        addresses__billing.address__is_current_record::BOOL,
        addresses__shipping.address__is_current_record::BOOL
    ]
  ) AS bridge__is_current_record
FROM silver.bag__adventure_works__sales_order_headers

LEFT JOIN silver.bag__adventure_works__customers
ON bag__adventure_works__sales_order_headers._hook__customer = bag__adventure_works__customers._hook__customer
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_from < bag__adventure_works__customers.customer__record_valid_to
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_to > bag__adventure_works__customers.customer__record_valid_from

LEFT JOIN silver.bag__adventure_works__addresses AS addresses__billing
ON bag__adventure_works__sales_order_headers._hook__address__billing = addresses__billing._hook__address
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_from < addresses__billing.address__record_valid_to
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_to > addresses__billing.address__record_valid_from

LEFT JOIN silver.bag__adventure_works__addresses AS addresses__shipping
ON bag__adventure_works__sales_order_headers._hook__address__shipping = addresses__shipping._hook__address
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_from < addresses__shipping.address__record_valid_to
AND bag__adventure_works__sales_order_headers.sales_order_header__record_valid_to > addresses__shipping.address__record_valid_from

WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
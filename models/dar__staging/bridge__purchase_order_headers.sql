MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__order__purchase, _pit_hook__person__employee, _pit_hook__ship_method, _pit_hook__vendor)
);

WITH cte__bridge AS (
  SELECT
    'purchase_order_headers' AS peripheral,
    _pit_hook__order__purchase,
    _hook__order__purchase,
    _hook__person__employee,
    _hook__vendor,
    _hook__ship_method,
    purchase_order_header__record_loaded_at AS bridge__record_loaded_at,
    purchase_order_header__record_updated_at AS bridge__record_updated_at,
    purchase_order_header__record_valid_from AS bridge__record_valid_from,
    purchase_order_header__record_valid_to AS bridge__record_valid_to,
    purchase_order_header__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__purchase_order_headers
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order__purchase,
    bridge__employees._pit_hook__person__employee,
    bridge__ship_methods._pit_hook__ship_method,
    bridge__vendors._pit_hook__vendor,
    cte__bridge._hook__order__purchase,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__employees.bridge__record_loaded_at,
        bridge__vendors.bridge__record_loaded_at,
        bridge__ship_methods.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__employees.bridge__record_updated_at,
        bridge__vendors.bridge__record_updated_at,
        bridge__ship_methods.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__employees.bridge__record_valid_from,
        bridge__vendors.bridge__record_valid_from,
        bridge__ship_methods.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__employees.bridge__record_valid_to,
        bridge__vendors.bridge__record_valid_to,
        bridge__ship_methods.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__employees.bridge__is_current_record,
          bridge__vendors.bridge__is_current_record,
          bridge__ship_methods.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__employees
  ON cte__bridge._hook__person__employee = bridge__employees._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= bridge__employees.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__employees.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__vendors
  ON cte__bridge._hook__vendor = bridge__vendors._hook__vendor
  AND cte__bridge.bridge__record_valid_from >= bridge__vendors.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__vendors.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__ship_methods
  ON cte__bridge._hook__ship_method = bridge__ship_methods._hook__ship_method
  AND cte__bridge.bridge__record_valid_from >= bridge__ship_methods.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__ship_methods.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__order__purchase::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__ship_method::TEXT,
      _pit_hook__vendor::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__order__purchase::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
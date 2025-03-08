MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__department, _pit_hook__order__purchase, _pit_hook__person__employee, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__reference__shift, _pit_hook__reference__unit_measure, _pit_hook__ship_method, _pit_hook__vendor)
);

WITH cte__bridge AS (
  SELECT
    'purchase_order_headers' AS peripheral,
    _pit_hook__order__purchase,
    _hook__order__purchase,
    _hook__person__employee,
    _hook__person__employee,
    _hook__person__employee,
    _hook__vendor,
    _hook__vendor,
    _hook__ship_method,
    _hook__epoch__date,
    measure__purchase_order_headers_placed,
    measure__purchase_order_headers_shipped,
    measure__purchase_order_headers_modified,
    purchase_order_header__record_loaded_at AS bridge__record_loaded_at,
    purchase_order_header__record_updated_at AS bridge__record_updated_at,
    purchase_order_header__record_valid_from AS bridge__record_valid_from,
    purchase_order_header__record_valid_to AS bridge__record_valid_to,
    purchase_order_header__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__purchase_order_headers
  LEFT JOIN dar__staging.measure__adventure_works__purchase_order_headers USING (_pit_hook__order__purchase)
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__order__purchase,
    uss_bridge__employee_department_histories._pit_hook__department,
    uss_bridge__employee_department_histories._pit_hook__person__employee,
    uss_bridge__employee_department_histories._pit_hook__reference__shift,
    uss_bridge__employee_pay_histories._pit_hook__person__employee,
    uss_bridge__employees._pit_hook__person__employee,
    uss_bridge__product_vendors._pit_hook__product,
    uss_bridge__product_vendors._pit_hook__product_category,
    uss_bridge__product_vendors._pit_hook__product_subcategory,
    uss_bridge__product_vendors._pit_hook__reference__product_model,
    uss_bridge__product_vendors._pit_hook__reference__unit_measure,
    uss_bridge__product_vendors._pit_hook__vendor,
    uss_bridge__ship_methods._pit_hook__ship_method,
    uss_bridge__vendors._pit_hook__vendor,
    cte__bridge._hook__order__purchase,
    cte__bridge._hook__epoch__date,
    cte__bridge.measure__purchase_order_headers_placed,
    cte__bridge.measure__purchase_order_headers_shipped,
    cte__bridge.measure__purchase_order_headers_modified,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__employees.bridge__record_loaded_at,
        uss_bridge__employee_pay_histories.bridge__record_loaded_at,
        uss_bridge__employee_department_histories.bridge__record_loaded_at,
        uss_bridge__vendors.bridge__record_loaded_at,
        uss_bridge__product_vendors.bridge__record_loaded_at,
        uss_bridge__ship_methods.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__employees.bridge__record_updated_at,
        uss_bridge__employee_pay_histories.bridge__record_updated_at,
        uss_bridge__employee_department_histories.bridge__record_updated_at,
        uss_bridge__vendors.bridge__record_updated_at,
        uss_bridge__product_vendors.bridge__record_updated_at,
        uss_bridge__ship_methods.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__employees.bridge__record_valid_from,
        uss_bridge__employee_pay_histories.bridge__record_valid_from,
        uss_bridge__employee_department_histories.bridge__record_valid_from,
        uss_bridge__vendors.bridge__record_valid_from,
        uss_bridge__product_vendors.bridge__record_valid_from,
        uss_bridge__ship_methods.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__employees.bridge__record_valid_to,
        uss_bridge__employee_pay_histories.bridge__record_valid_to,
        uss_bridge__employee_department_histories.bridge__record_valid_to,
        uss_bridge__vendors.bridge__record_valid_to,
        uss_bridge__product_vendors.bridge__record_valid_to,
        uss_bridge__ship_methods.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          uss_bridge__employees.bridge__is_current_record,
          uss_bridge__employee_pay_histories.bridge__is_current_record,
          uss_bridge__employee_department_histories.bridge__is_current_record,
          uss_bridge__vendors.bridge__is_current_record,
          uss_bridge__product_vendors.bridge__is_current_record,
          uss_bridge__ship_methods.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.uss_bridge__employees
  ON cte__bridge._hook__person__employee = uss_bridge__employees._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__employees.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__employees.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__employee_pay_histories
  ON cte__bridge._hook__person__employee = uss_bridge__employee_pay_histories._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__employee_pay_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__employee_pay_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__employee_department_histories
  ON cte__bridge._hook__person__employee = uss_bridge__employee_department_histories._hook__person__employee
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__employee_department_histories.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__employee_department_histories.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__vendors
  ON cte__bridge._hook__vendor = uss_bridge__vendors._hook__vendor
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__vendors.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__vendors.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__product_vendors
  ON cte__bridge._hook__vendor = uss_bridge__product_vendors._hook__vendor
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__product_vendors.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__product_vendors.bridge__record_valid_to
  LEFT JOIN dar__staging.uss_bridge__ship_methods
  ON cte__bridge._hook__ship_method = uss_bridge__ship_methods._hook__ship_method
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__ship_methods.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__ship_methods.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _hook__epoch__date::TEXT,
      _pit_hook__department::TEXT,
      _pit_hook__order__purchase::TEXT,
      _pit_hook__person__employee::TEXT,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__product_model::TEXT,
      _pit_hook__reference__shift::TEXT,
      _pit_hook__reference__unit_measure::TEXT,
      _pit_hook__ship_method::TEXT,
      _pit_hook__vendor::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__shift::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__order__purchase::BLOB,
  _hook__epoch__date::BLOB,
  measure__purchase_order_headers_placed::INT,
  measure__purchase_order_headers_shipped::INT,
  measure__purchase_order_headers_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
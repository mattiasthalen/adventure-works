MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__credit_card,
    _pit_hook__currency,
    _pit_hook__customer,
    _pit_hook__order__sales,
    _pit_hook__order_line__sales,
    _pit_hook__person__sales,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__country_region,
    _pit_hook__reference__product_model,
    _pit_hook__reference__special_offer,
    _pit_hook__ship_method,
    _pit_hook__store,
    _pit_hook__territory__sales,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__credit_card,
    _pit_hook__currency,
    _pit_hook__customer,
    _pit_hook__order__sales,
    _pit_hook__order_line__sales,
    _pit_hook__person__sales,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__country_region,
    _pit_hook__reference__product_model,
    _pit_hook__reference__special_offer,
    _pit_hook__ship_method,
    _pit_hook__store,
    _pit_hook__territory__sales,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__sales_order_details
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__order_line__sales,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'sales_order_detail__modified_date' THEN 1 END) AS event__sales_order_details_modified
  FROM dab.bag__adventure_works__sales_order_details
  UNPIVOT (
    event_date FOR event IN (
      sales_order_detail__modified_date
    )
  ) AS pivot__events
  GROUP BY ALL
  ORDER BY _hook__epoch__date
),
final AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      _pit_hook__bridge::TEXT,
      _hook__epoch__date::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
  LEFT JOIN cte__events USING(_pit_hook__order_line__sales)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__credit_card::BLOB,
  _pit_hook__currency::BLOB,
  _pit_hook__customer::BLOB,
  _pit_hook__order__sales::BLOB,
  _pit_hook__order_line__sales::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__special_offer::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__epoch__date::BLOB,
  event__sales_order_details_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
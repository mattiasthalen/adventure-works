MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__department,
    _pit_hook__order__purchase,
    _pit_hook__order_line__purchase,
    _pit_hook__person__employee,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__product_model,
    _pit_hook__reference__shift,
    _pit_hook__reference__unit_measure,
    _pit_hook__ship_method,
    _pit_hook__vendor,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__department,
    _pit_hook__order__purchase,
    _pit_hook__order_line__purchase,
    _pit_hook__person__employee,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__product_model,
    _pit_hook__reference__shift,
    _pit_hook__reference__unit_measure,
    _pit_hook__ship_method,
    _pit_hook__vendor,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__purchase_order_details
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__order_line__purchase,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'purchase_order_detail__due_date' THEN 1 END) AS event__purchase_order_details_due,
    MAX(CASE WHEN pivot__events.event = 'purchase_order_detail__modified_date' THEN 1 END) AS event__purchase_order_details_modified
  FROM dab.bag__adventure_works__purchase_order_details
  UNPIVOT (
    event_date FOR event IN (
      purchase_order_detail__due_date,
      purchase_order_detail__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__order_line__purchase)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__order_line__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__shift::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__epoch__date::BLOB,
  event__purchase_order_details_due::INT,
  event__purchase_order_details_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
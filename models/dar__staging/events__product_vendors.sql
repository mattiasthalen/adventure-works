MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__product_vendor,
    _pit_hook__reference__product_model,
    _pit_hook__reference__unit_measure,
    _pit_hook__vendor,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__product_vendor,
    _pit_hook__reference__product_model,
    _pit_hook__reference__unit_measure,
    _pit_hook__vendor,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__product_vendors
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__product_vendor,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'product_vendor__last_receipt_date' THEN 1 END) AS event__product_vendors_last_receipt,
    MAX(CASE WHEN pivot__events.event = 'product_vendor__modified_date' THEN 1 END) AS event__product_vendors_modified
  FROM dab.bag__adventure_works__product_vendors
  UNPIVOT (
    event_date FOR event IN (
      product_vendor__last_receipt_date,
      product_vendor__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__product_vendor)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__product_vendor::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__epoch__date::BLOB,
  event__product_vendors_last_receipt::INT,
  event__product_vendors_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
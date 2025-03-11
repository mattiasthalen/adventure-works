MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__order__work,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__location,
    _pit_hook__reference__product_model,
    _pit_hook__reference__scrap_reason,
    _pit_hook__work_order_routing,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__order__work,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__location,
    _pit_hook__reference__product_model,
    _pit_hook__reference__scrap_reason,
    _pit_hook__work_order_routing,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__work_order_routings
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__work_order_routing,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'work_order_routing__scheduled_start_date' THEN 1 END) AS event__work_order_routings_scheduled_started,
    MAX(CASE WHEN pivot__events.event = 'work_order_routing__scheduled_end_date' THEN 1 END) AS event__work_order_routings_scheduled_ended,
    MAX(CASE WHEN pivot__events.event = 'work_order_routing__actual_start_date' THEN 1 END) AS event__work_order_routings_actual_started,
    MAX(CASE WHEN pivot__events.event = 'work_order_routing__actual_end_date' THEN 1 END) AS event__work_order_routings_actual_ended,
    MAX(CASE WHEN pivot__events.event = 'work_order_routing__modified_date' THEN 1 END) AS event__work_order_routings_modified
  FROM dab.bag__adventure_works__work_order_routings
  UNPIVOT (
    event_date FOR event IN (
      work_order_routing__scheduled_start_date,
      work_order_routing__scheduled_end_date,
      work_order_routing__actual_start_date,
      work_order_routing__actual_end_date,
      work_order_routing__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__work_order_routing)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__order__work::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__location::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__scrap_reason::BLOB,
  _pit_hook__work_order_routing::BLOB,
  _hook__epoch__date::BLOB,
  event__work_order_routings_scheduled_started::INT,
  event__work_order_routings_scheduled_ended::INT,
  event__work_order_routings_actual_started::INT,
  event__work_order_routings_actual_ended::INT,
  event__work_order_routings_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__order__purchase,
    _pit_hook__person__employee,
    _pit_hook__ship_method,
    _pit_hook__vendor,
    _hook__epoch__date
  ),
  description 'Event viewpoint of purchase_order_headers data: General purchase order information. See PurchaseOrderDetail.',
  column_descriptions (
    peripheral = 'Name of the purchase_order_headers peripheral this event relates to',
    _pit_hook__bridge = 'Unique identifier for this event record',
    _hook__epoch__date = 'Hook to the date the event occurred',
    event__purchase_order_headers_placed = 'Flag indicating a placed event for this purchase_order_headers',
    event__purchase_order_headers_shipped = 'Flag indicating a shipped event for this purchase_order_headers',
    event__purchase_order_headers_modified = 'Flag indicating a modified event for this purchase_order_headers',
    bridge__record_loaded_at = 'Timestamp when this event record was loaded',
    bridge__record_updated_at = 'Timestamp when this event record was last updated',
    bridge__record_valid_from = 'Timestamp from which this event record is valid',
    bridge__record_valid_to = 'Timestamp until which this event record is valid',
    bridge__is_current_record = 'Flag indicating if this is the current valid version of the event record'
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__order__purchase,
    _pit_hook__person__employee,
    _pit_hook__ship_method,
    _pit_hook__vendor,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__purchase_order_headers
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__order__purchase,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'purchase_order_header__order_date' THEN 1 END) AS event__purchase_order_headers_placed,
    MAX(CASE WHEN pivot__events.event = 'purchase_order_header__ship_date' THEN 1 END) AS event__purchase_order_headers_shipped,
    MAX(CASE WHEN pivot__events.event = 'purchase_order_header__modified_date' THEN 1 END) AS event__purchase_order_headers_modified
  FROM dab.bag__adventure_works__purchase_order_headers
  UNPIVOT (
    event_date FOR event IN (
      purchase_order_header__order_date,
      purchase_order_header__ship_date,
      purchase_order_header__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__order__purchase)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__vendor::BLOB,
  _hook__epoch__date::BLOB,
  event__purchase_order_headers_placed::INT,
  event__purchase_order_headers_shipped::INT,
  event__purchase_order_headers_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
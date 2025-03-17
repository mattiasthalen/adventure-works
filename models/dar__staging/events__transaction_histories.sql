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
    _pit_hook__reference__product_model,
    _pit_hook__transaction_history,
    _hook__epoch__date
  ),
  description 'Event viewpoint of transaction_histories data: Record of each purchase order, sales order, or work order transaction year to date.',
  column_descriptions (
    peripheral = 'Name of the transaction_histories peripheral this event relates to',
    _pit_hook__bridge = 'Unique identifier for this event record',
    _hook__epoch__date = 'Hook to the date the event occurred',
    event__transaction_histories_transaction = 'Flag indicating a transaction event for this transaction_histories',
    event__transaction_histories_modified = 'Flag indicating a modified event for this transaction_histories',
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
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__product_model,
    _pit_hook__transaction_history,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__transaction_histories
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__transaction_history,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'transaction_history__transaction_date' THEN 1 END) AS event__transaction_histories_transaction,
    MAX(CASE WHEN pivot__events.event = 'transaction_history__modified_date' THEN 1 END) AS event__transaction_histories_modified
  FROM dab.bag__adventure_works__transaction_histories
  UNPIVOT (
    event_date FOR event IN (
      transaction_history__transaction_date,
      transaction_history__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__transaction_history)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__transaction_history::BLOB,
  _hook__epoch__date::BLOB,
  event__transaction_histories_transaction::INT,
  event__transaction_histories_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
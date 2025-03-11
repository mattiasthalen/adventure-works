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
    _pit_hook__product_list_price_history,
    _pit_hook__product_subcategory,
    _pit_hook__reference__product_model,
    _hook__epoch__date
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_list_price_history,
    _pit_hook__product_subcategory,
    _pit_hook__reference__product_model,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__product_list_price_histories
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__product_list_price_history,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'product_list_price_history__start_date' THEN 1 END) AS event__product_list_price_histories_started,
    MAX(CASE WHEN pivot__events.event = 'product_list_price_history__end_date' THEN 1 END) AS event__product_list_price_histories_ended,
    MAX(CASE WHEN pivot__events.event = 'product_list_price_history__modified_date' THEN 1 END) AS event__product_list_price_histories_modified
  FROM dab.bag__adventure_works__product_list_price_histories
  UNPIVOT (
    event_date FOR event IN (
      product_list_price_history__start_date,
      product_list_price_history__end_date,
      product_list_price_history__modified_date
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
  LEFT JOIN cte__events USING(_pit_hook__product_list_price_history)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_list_price_history::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _hook__epoch__date::BLOB,
  event__product_list_price_histories_started::INT,
  event__product_list_price_histories_ended::INT,
  event__product_list_price_histories_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
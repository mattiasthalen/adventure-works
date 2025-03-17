MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__person__sales,
    _pit_hook__reference__country_region,
    _pit_hook__territory__sales,
    _hook__epoch__date
  ),
  description 'Event viewpoint of sales_territory_histories data: Sales representative transfers to other sales territories.',
  column_descriptions (
    peripheral = 'Name of the sales_territory_histories peripheral this event relates to',
    _pit_hook__bridge = 'Unique identifier for this event record',
    _hook__epoch__date = 'Hook to the date the event occurred',
    event__sales_territory_histories_started = 'Flag indicating a started event for this sales_territory_histories',
    event__sales_territory_histories_modified = 'Flag indicating a modified event for this sales_territory_histories',
    event__sales_territory_histories_ended = 'Flag indicating a ended event for this sales_territory_histories',
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
    _pit_hook__person__sales,
    _pit_hook__reference__country_region,
    _pit_hook__territory__sales,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__sales_territory_histories
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__person__sales,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'sales_territory_history__start_date' THEN 1 END) AS event__sales_territory_histories_started,
    MAX(CASE WHEN pivot__events.event = 'sales_territory_history__modified_date' THEN 1 END) AS event__sales_territory_histories_modified,
    MAX(CASE WHEN pivot__events.event = 'sales_territory_history__end_date' THEN 1 END) AS event__sales_territory_histories_ended
  FROM dab.bag__adventure_works__sales_territory_histories
  UNPIVOT (
    event_date FOR event IN (
      sales_territory_history__start_date,
      sales_territory_history__modified_date,
      sales_territory_history__end_date
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
  LEFT JOIN cte__events USING(_pit_hook__person__sales)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__epoch__date::BLOB,
  event__sales_territory_histories_started::INT,
  event__sales_territory_histories_modified::INT,
  event__sales_territory_histories_ended::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
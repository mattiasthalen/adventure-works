MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__department,
    _pit_hook__employee_department_history,
    _pit_hook__person__employee,
    _pit_hook__reference__shift,
    _hook__epoch__date
  ),
  description 'Event viewpoint of employee_department_histories data: Employee department transfers.',
  column_descriptions (
    peripheral = 'Name of the employee_department_histories peripheral this event relates to',
    _pit_hook__bridge = 'Unique identifier for this event record',
    _hook__epoch__date = 'Hook to the date the event occurred',
    event__employee_department_histories_started = 'Flag indicating a started event for this employee_department_histories',
    event__employee_department_histories_modified = 'Flag indicating a modified event for this employee_department_histories',
    event__employee_department_histories_ended = 'Flag indicating a ended event for this employee_department_histories',
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
    _pit_hook__department,
    _pit_hook__employee_department_history,
    _pit_hook__person__employee,
    _pit_hook__reference__shift,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__employee_department_histories
),
cte__events AS (
  SELECT
    pivot__events._pit_hook__employee_department_history,
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
    MAX(CASE WHEN pivot__events.event = 'employee_department_history__start_date' THEN 1 END) AS event__employee_department_histories_started,
    MAX(CASE WHEN pivot__events.event = 'employee_department_history__modified_date' THEN 1 END) AS event__employee_department_histories_modified,
    MAX(CASE WHEN pivot__events.event = 'employee_department_history__end_date' THEN 1 END) AS event__employee_department_histories_ended
  FROM dab.bag__adventure_works__employee_department_histories
  UNPIVOT (
    event_date FOR event IN (
      employee_department_history__start_date,
      employee_department_history__modified_date,
      employee_department_history__end_date
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
  LEFT JOIN cte__events USING(_pit_hook__employee_department_history)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__employee_department_history::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__reference__shift::BLOB,
  _hook__epoch__date::BLOB,
  event__employee_department_histories_started::INT,
  event__employee_department_histories_modified::INT,
  event__employee_department_histories_ended::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts
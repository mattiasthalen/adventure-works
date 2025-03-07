MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order_line__work,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__order_line__work, _hook__epoch__date),
  references (_pit_hook__order_line__work, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__scheduled_start_date,
    work_order_routing__scheduled_end_date,
    work_order_routing__actual_start_date,
    work_order_routing__actual_end_date,
    work_order_routing__modified_date
  FROM dab.bag__adventure_works__work_order_routings
  WHERE 1 = 1
  AND work_order_routing__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__scheduled_start_date AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__scheduled_start_date::DATE AS measure_date,
    1 AS measure__work_order_routings_scheduled_start
  FROM cte__source
  WHERE work_order_routing__scheduled_start_date IS NOT NULL
), cte__scheduled_end_date AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__scheduled_end_date::DATE AS measure_date,
    1 AS measure__work_order_routings_scheduled_end
  FROM cte__source
  WHERE work_order_routing__scheduled_end_date IS NOT NULL
), cte__actual_start_date AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__actual_start_date::DATE AS measure_date,
    1 AS measure__work_order_routings_actual_start
  FROM cte__source
  WHERE work_order_routing__actual_start_date IS NOT NULL
), cte__actual_end_date AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__actual_end_date::DATE AS measure_date,
    1 AS measure__work_order_routings_actual_end
  FROM cte__source
  WHERE work_order_routing__actual_end_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__order_line__work,
    work_order_routing__modified_date::DATE AS measure_date,
    1 AS measure__work_order_routings_modified
  FROM cte__source
  WHERE work_order_routing__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__scheduled_start_date
  FULL OUTER JOIN cte__scheduled_end_date USING (_pit_hook__order_line__work, measure_date)
  FULL OUTER JOIN cte__actual_start_date USING (_pit_hook__order_line__work, measure_date)
  FULL OUTER JOIN cte__actual_end_date USING (_pit_hook__order_line__work, measure_date)
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__order_line__work, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__order_line__work::BLOB,
  _hook__epoch__date::BLOB,
  measure__work_order_routings_scheduled_start::INT,
  measure__work_order_routings_scheduled_end::INT,
  measure__work_order_routings_actual_start::INT,
  measure__work_order_routings_actual_end::INT,
  measure__work_order_routings_modified::INT
FROM cte__epoch
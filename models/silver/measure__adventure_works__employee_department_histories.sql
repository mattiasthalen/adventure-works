MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__employee,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__employee, _hook__epoch__date),
  references (_pit_hook__person__employee, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__person__employee,
    employee_department_history__start_date,
    employee_department_history__end_date
  FROM silver.bag__adventure_works__employee_department_histories
  WHERE 1 = 1
  AND employee_department_history__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__start_date AS (
  SELECT
    _pit_hook__person__employee,
    employee_department_history__start_date::DATE AS measure_date,
    1 AS measure__employee_department_histories_started
  FROM cte__source
  WHERE employee_department_history__start_date IS NOT NULL
), cte__end_date AS (
  SELECT
    _pit_hook__person__employee,
    employee_department_history__end_date::DATE AS measure_date,
    1 AS measure__employee_department_histories_finished
  FROM cte__source
  WHERE employee_department_history__end_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__start_date
  FULL OUTER JOIN cte__end_date USING (_pit_hook__person__employee, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__person__employee::BLOB,
  _hook__epoch__date::BLOB,
  measure__employee_department_histories_started::INT,
  measure__employee_department_histories_finished::INT
FROM cte__epoch
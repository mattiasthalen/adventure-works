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
    employee__birth_date,
    employee__hire_date,
    employee__modified_date
  FROM dab.bag__adventure_works__employees
  WHERE 1 = 1
  AND employee__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__birth_date AS (
  SELECT
    _pit_hook__person__employee,
    employee__birth_date AS measure_date,
    1 AS measure__employees_birth
  FROM cte__source
  WHERE employee__birth_date IS NOT NULL
), cte__hire_date AS (
  SELECT
    _pit_hook__person__employee,
    employee__hire_date AS measure_date,
    1 AS measure__employees_hire
  FROM cte__source
  WHERE employee__hire_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__person__employee,
    employee__modified_date AS measure_date,
    1 AS measure__employees_modified
  FROM cte__source
  WHERE employee__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__birth_date
  FULL OUTER JOIN cte__hire_date USING (_pit_hook__person__employee, measure_date)
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__person__employee, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__person__employee::BLOB,
  _hook__epoch__date::BLOB,
  measure__employees_birth::INT,
  measure__employees_hire::INT,
  measure__employees_modified::INT
FROM cte__epoch
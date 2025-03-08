MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bill_of_materials,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__bill_of_materials, _hook__epoch__date),
  references (_pit_hook__bill_of_materials, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__bill_of_materials,
    bill_of_material__start_date,
    bill_of_material__modified_date,
    bill_of_material__end_date
  FROM dab.bag__adventure_works__bill_of_materials
  WHERE 1 = 1
  AND bill_of_material__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__start_date AS (
  SELECT
    _pit_hook__bill_of_materials,
    bill_of_material__start_date AS measure_date,
    1 AS measure__bill_of_materials_started
  FROM cte__source
  WHERE bill_of_material__start_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__bill_of_materials,
    bill_of_material__modified_date AS measure_date,
    1 AS measure__bill_of_materials_modified
  FROM cte__source
  WHERE bill_of_material__modified_date IS NOT NULL
), cte__end_date AS (
  SELECT
    _pit_hook__bill_of_materials,
    bill_of_material__end_date AS measure_date,
    1 AS measure__bill_of_materials_finished
  FROM cte__source
  WHERE bill_of_material__end_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__start_date
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__bill_of_materials, measure_date)
  FULL OUTER JOIN cte__end_date USING (_pit_hook__bill_of_materials, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__bill_of_materials::BLOB,
  _hook__epoch__date::BLOB,
  measure__bill_of_materials_started::INT,
  measure__bill_of_materials_modified::INT,
  measure__bill_of_materials_finished::INT
FROM cte__epoch
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__sales
  ),
  tags hook,
  grain (_pit_hook__order__sales, _hook__epoch__date),
  references (_pit_hook__order__sales, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__order_date,
    sales_order_header__due_date,
    sales_order_header__ship_date,
    sales_order_header__modified_date,
    sales_order_header__record_updated_at
  FROM silver.bag__adventure_works__sales_order_headers
), cte__order_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__order_date::DATE AS measure_date,
    1 AS measure__sales_order_placed,
    sales_order_header__record_updated_at
  FROM cte__source
  WHERE sales_order_header__order_date IS NOT NULL
), cte__due_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__due_date::DATE AS measure_date,
    1 AS measure__sales_order_due,
    sales_order_header__record_updated_at
  FROM cte__source
  WHERE sales_order_header__due_date IS NOT NULL
), cte__ship_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__ship_date::DATE AS measure_date,
    1 AS measure__sales_order_shipped,
    sales_order_header__record_updated_at
  FROM cte__source
  WHERE sales_order_header__ship_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__modified_date::DATE AS measure_date,
    1 AS measure__sales_order_modified,
    sales_order_header__record_updated_at
  FROM cte__source
  WHERE sales_order_header__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__order_date
  FULL OUTER JOIN cte__due_date USING (_pit_hook__order__sales, measure_date)
  FULL OUTER JOIN cte__ship_date USING (_pit_hook__order__sales, measure_date)
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__order__sales, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__order__sales::BLOB,
  _hook__epoch__date::BLOB,
  measure__sales_order_placed::INT,
  measure__sales_order_due::INT,
  measure__sales_order_shipped::INT,
  measure__sales_order_modified::INT
FROM cte__epoch
WHERE 1 = 1
AND sales_order_header__record_updated_at BETWEEN @start_ts AND @end_ts
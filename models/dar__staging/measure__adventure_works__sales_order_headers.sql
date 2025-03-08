MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__sales,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
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
    sales_order_header__modified_date
  FROM dab.bag__adventure_works__sales_order_headers
  WHERE 1 = 1
  AND sales_order_header__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__order_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__order_date AS measure_date,
    1 AS measure__sales_order_headers_placed
  FROM cte__source
  WHERE sales_order_header__order_date IS NOT NULL
), cte__due_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__due_date AS measure_date,
    1 AS measure__sales_order_headers_due
  FROM cte__source
  WHERE sales_order_header__due_date IS NOT NULL
), cte__ship_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__ship_date AS measure_date,
    1 AS measure__sales_order_headers_shipped
  FROM cte__source
  WHERE sales_order_header__ship_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__modified_date AS measure_date,
    1 AS measure__sales_order_headers_modified
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
  measure__sales_order_headers_placed::INT,
  measure__sales_order_headers_due::INT,
  measure__sales_order_headers_shipped::INT,
  measure__sales_order_headers_modified::INT
FROM cte__epoch
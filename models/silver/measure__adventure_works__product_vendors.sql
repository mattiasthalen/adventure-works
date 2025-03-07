MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__vendor,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__vendor, _hook__epoch__date),
  references (_pit_hook__vendor, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit_hook__vendor,
    product_vendor__last_receipt_date,
    product_vendor__modified_date
  FROM silver.bag__adventure_works__product_vendors
  WHERE 1 = 1
  AND product_vendor__record_updated_at BETWEEN @start_ts AND @end_ts
), cte__last_receipt_date AS (
  SELECT
    _pit_hook__vendor,
    product_vendor__last_receipt_date::DATE AS measure_date,
    1 AS measure__product_vendors_last_receipt
  FROM cte__source
  WHERE product_vendor__last_receipt_date IS NOT NULL
), cte__modified_date AS (
  SELECT
    _pit_hook__vendor,
    product_vendor__modified_date::DATE AS measure_date,
    1 AS measure__product_vendors_modified
  FROM cte__source
  WHERE product_vendor__modified_date IS NOT NULL
), cte__measures AS (
  SELECT
    *
  FROM cte__last_receipt_date
  FULL OUTER JOIN cte__modified_date USING (_pit_hook__vendor, measure_date)
), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit_hook__vendor::BLOB,
  _hook__epoch__date::BLOB,
  measure__product_vendors_last_receipt::INT,
  measure__product_vendors_modified::INT
FROM cte__epoch
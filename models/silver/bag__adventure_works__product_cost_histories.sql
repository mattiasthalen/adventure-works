MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_cost_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_id AS product_cost_histori__product_id,
    end_date AS product_cost_histori__end_date,
    modified_date AS product_cost_histori__modified_date,
    standard_cost AS product_cost_histori__standard_cost,
    start_date AS product_cost_histori__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_cost_histori__record_loaded_at
  FROM bronze.raw__adventure_works__product_cost_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_cost_histori__product_id ORDER BY product_cost_histori__record_loaded_at) AS product_cost_histori__record_version,
    CASE
      WHEN product_cost_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_cost_histori__record_loaded_at
    END AS product_cost_histori__record_valid_from,
    COALESCE(
      LEAD(product_cost_histori__record_loaded_at) OVER (PARTITION BY product_cost_histori__product_id ORDER BY product_cost_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_cost_histori__record_valid_to,
    product_cost_histori__record_valid_to = @max_ts::TIMESTAMP AS product_cost_histori__is_current_record,
    CASE
      WHEN product_cost_histori__is_current_record
      THEN product_cost_histori__record_loaded_at
      ELSE product_cost_histori__record_valid_to
    END AS product_cost_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product_cost_histori__product_id,
      '~epoch|valid_from|',
      product_cost_histori__record_valid_from
    ) AS _pit_hook__product,
    CONCAT('product|adventure_works|', product_cost_histori__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  product_cost_histori__product_id::BIGINT,
  product_cost_histori__end_date::VARCHAR,
  product_cost_histori__modified_date::VARCHAR,
  product_cost_histori__standard_cost::DOUBLE,
  product_cost_histori__start_date::TIMESTAMP,
  product_cost_histori__record_loaded_at::TIMESTAMP,
  product_cost_histori__record_updated_at::TIMESTAMP,
  product_cost_histori__record_valid_from::TIMESTAMP,
  product_cost_histori__record_valid_to::TIMESTAMP,
  product_cost_histori__record_version::INT,
  product_cost_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_cost_histori__record_updated_at BETWEEN @start_ts AND @end_ts
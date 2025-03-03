MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_id AS product__product_id,
    end_date AS product__end_date,
    modified_date AS product__modified_date,
    standard_cost AS product__standard_cost,
    start_date AS product__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product__record_loaded_at
  FROM bronze.raw__adventure_works__product_cost_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at) AS product__record_version,
    CASE
      WHEN product__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product__record_loaded_at
    END AS product__record_valid_from,
    COALESCE(
      LEAD(product__record_loaded_at) OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product__record_valid_to,
    product__record_valid_to = @max_ts::TIMESTAMP AS product__is_current_record,
    CASE
      WHEN product__is_current_record
      THEN product__record_loaded_at
      ELSE product__record_valid_to
    END AS product__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product__product_id,
      '~epoch|valid_from|',
      product__record_valid_from
    ) AS _pit_hook__product,
    CONCAT('product|adventure_works|', product__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  product__product_id::VARCHAR,
  product__end_date::VARCHAR,
  product__modified_date::VARCHAR,
  product__standard_cost::VARCHAR,
  product__start_date::VARCHAR,
  product__record_loaded_at::TIMESTAMP,
  product__record_version::INT,
  product__record_valid_from::TIMESTAMP,
  product__record_valid_to::TIMESTAMP,
  product__is_current_record::BOOLEAN,
  product__record_updated_at::TIMESTAMP
FROM hooks
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_model__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_model_id AS product_model__product_model_id,
    catalog_description AS product_model__catalog_description,
    instructions AS product_model__instructions,
    modified_date AS product_model__modified_date,
    name AS product_model__name,
    rowguid AS product_model__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_model__record_loaded_at
  FROM bronze.raw__adventure_works__product_models
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_model__product_model_id ORDER BY product_model__record_loaded_at) AS product_model__record_version,
    CASE
      WHEN product_model__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_model__record_loaded_at
    END AS product_model__record_valid_from,
    COALESCE(
      LEAD(product_model__record_loaded_at) OVER (PARTITION BY product_model__product_model_id ORDER BY product_model__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_model__record_valid_to,
    product_model__record_valid_to = @max_ts::TIMESTAMP AS product_model__is_current_record,
    CASE
      WHEN product_model__is_current_record
      THEN product_model__record_loaded_at
      ELSE product_model__record_valid_to
    END AS product_model__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_model|adventure_works|',
      product_model__product_model_id,
      '~epoch|valid_from|',
      product_model__record_valid_from
    ) AS _pit_hook__product_model,
    CONCAT('product_model|adventure_works|', product_model__product_model_id) AS _hook__product_model,
    *
  FROM validity
)
SELECT
  _pit_hook__product_model::BLOB,
  _hook__product_model::BLOB,
  product_model__product_model_id::BIGINT,
  product_model__catalog_description::VARCHAR,
  product_model__instructions::VARCHAR,
  product_model__modified_date::VARCHAR,
  product_model__name::VARCHAR,
  product_model__rowguid::VARCHAR,
  product_model__record_loaded_at::TIMESTAMP,
  product_model__record_updated_at::TIMESTAMP,
  product_model__record_valid_from::TIMESTAMP,
  product_model__record_valid_to::TIMESTAMP,
  product_model__record_version::INT,
  product_model__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_model__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_categori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_category_id AS product_categori__product_category_id,
    modified_date AS product_categori__modified_date,
    name AS product_categori__name,
    rowguid AS product_categori__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_categori__record_loaded_at
  FROM bronze.raw__adventure_works__product_categories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_categori__product_category_id ORDER BY product_categori__record_loaded_at) AS product_categori__record_version,
    CASE
      WHEN product_categori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_categori__record_loaded_at
    END AS product_categori__record_valid_from,
    COALESCE(
      LEAD(product_categori__record_loaded_at) OVER (PARTITION BY product_categori__product_category_id ORDER BY product_categori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_categori__record_valid_to,
    product_categori__record_valid_to = @max_ts::TIMESTAMP AS product_categori__is_current_record,
    CASE
      WHEN product_categori__is_current_record
      THEN product_categori__record_loaded_at
      ELSE product_categori__record_valid_to
    END AS product_categori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_category|adventure_works|',
      product_categori__product_category_id,
      '~epoch|valid_from|',
      product_categori__record_valid_from
    ) AS _pit_hook__product_category,
    CONCAT('product_category|adventure_works|', product_categori__product_category_id) AS _hook__product_category,
    *
  FROM validity
)
SELECT
  _pit_hook__product_category::BLOB,
  _hook__product_category::BLOB,
  product_categori__product_category_id::BIGINT,
  product_categori__modified_date::VARCHAR,
  product_categori__name::VARCHAR,
  product_categori__rowguid::VARCHAR,
  product_categori__record_loaded_at::TIMESTAMP,
  product_categori__record_updated_at::TIMESTAMP,
  product_categori__record_valid_from::TIMESTAMP,
  product_categori__record_valid_to::TIMESTAMP,
  product_categori__record_version::INT,
  product_categori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_categori__record_updated_at BETWEEN @start_ts AND @end_ts
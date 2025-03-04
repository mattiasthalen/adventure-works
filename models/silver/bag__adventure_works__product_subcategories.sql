MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_subcategori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_subcategory_id AS product_subcategori__product_subcategory_id,
    product_category_id AS product_subcategori__product_category_id,
    modified_date AS product_subcategori__modified_date,
    name AS product_subcategori__name,
    rowguid AS product_subcategori__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_subcategori__record_loaded_at
  FROM bronze.raw__adventure_works__product_subcategories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_subcategori__product_subcategory_id ORDER BY product_subcategori__record_loaded_at) AS product_subcategori__record_version,
    CASE
      WHEN product_subcategori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_subcategori__record_loaded_at
    END AS product_subcategori__record_valid_from,
    COALESCE(
      LEAD(product_subcategori__record_loaded_at) OVER (PARTITION BY product_subcategori__product_subcategory_id ORDER BY product_subcategori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_subcategori__record_valid_to,
    product_subcategori__record_valid_to = @max_ts::TIMESTAMP AS product_subcategori__is_current_record,
    CASE
      WHEN product_subcategori__is_current_record
      THEN product_subcategori__record_loaded_at
      ELSE product_subcategori__record_valid_to
    END AS product_subcategori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_subcategory|adventure_works|',
      product_subcategori__product_subcategory_id,
      '~epoch|valid_from|',
      product_subcategori__record_valid_from
    ) AS _pit_hook__product_subcategory,
    CONCAT('product_subcategory|adventure_works|', product_subcategori__product_subcategory_id) AS _hook__product_subcategory,
    CONCAT('product_category|adventure_works|', product_subcategori__product_category_id) AS _hook__product_category,
    *
  FROM validity
)
SELECT
  _pit_hook__product_subcategory::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__product_category::BLOB,
  product_subcategori__product_subcategory_id::BIGINT,
  product_subcategori__product_category_id::BIGINT,
  product_subcategori__modified_date::VARCHAR,
  product_subcategori__name::VARCHAR,
  product_subcategori__rowguid::VARCHAR,
  product_subcategori__record_loaded_at::TIMESTAMP,
  product_subcategori__record_updated_at::TIMESTAMP,
  product_subcategori__record_valid_from::TIMESTAMP,
  product_subcategori__record_valid_to::TIMESTAMP,
  product_subcategori__record_version::INT,
  product_subcategori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_subcategori__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_subcategory_id AS product_subcategory__product_subcategory_id,
    product_category_id AS product_subcategory__product_category_id,
    modified_date AS product_subcategory__modified_date,
    name AS product_subcategory__name,
    rowguid AS product_subcategory__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_subcategory__record_loaded_at
  FROM bronze.raw__adventure_works__product_subcategories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_subcategory__product_subcategory_id ORDER BY product_subcategory__record_loaded_at) AS product_subcategory__record_version,
    CASE
      WHEN product_subcategory__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_subcategory__record_loaded_at
    END AS product_subcategory__record_valid_from,
    COALESCE(
      LEAD(product_subcategory__record_loaded_at) OVER (PARTITION BY product_subcategory__product_subcategory_id ORDER BY product_subcategory__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_subcategory__record_valid_to,
    product_subcategory__record_valid_to = @max_ts::TIMESTAMP AS product_subcategory__is_current_record,
    CASE
      WHEN product_subcategory__is_current_record
      THEN product_subcategory__record_loaded_at
      ELSE product_subcategory__record_valid_to
    END AS product_subcategory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_subcategory|adventure_works|',
      product_subcategory__product_subcategory_id,
      '~epoch|valid_from|',
      product_subcategory__record_valid_from
    ) AS _pit_hook__product_subcategory,
    CONCAT('product_subcategory|adventure_works|', product_subcategory__product_subcategory_id) AS _hook__product_subcategory,
    CONCAT('product_category|adventure_works|', product_subcategory__product_category_id) AS _hook__product_category,
    *
  FROM validity
)
SELECT
  _pit_hook__product_subcategory::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__product_category::BLOB,
  product_subcategory__product_subcategory_id::VARCHAR,
  product_subcategory__product_category_id::VARCHAR,
  product_subcategory__modified_date::VARCHAR,
  product_subcategory__name::VARCHAR,
  product_subcategory__rowguid::VARCHAR,
  product_subcategory__record_loaded_at::TIMESTAMP,
  product_subcategory__record_version::INT,
  product_subcategory__record_valid_from::TIMESTAMP,
  product_subcategory__record_valid_to::TIMESTAMP,
  product_subcategory__is_current_record::BOOLEAN,
  product_subcategory__record_updated_at::TIMESTAMP
FROM hooks
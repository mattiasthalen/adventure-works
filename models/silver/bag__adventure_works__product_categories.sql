MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_category_id AS product_category__product_category_id,
    modified_date AS product_category__modified_date,
    name AS product_category__name,
    rowguid AS product_category__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_category__record_loaded_at
  FROM bronze.raw__adventure_works__product_categories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_category__product_category_id ORDER BY product_category__record_loaded_at) AS product_category__record_version,
    CASE
      WHEN product_category__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_category__record_loaded_at
    END AS product_category__record_valid_from,
    COALESCE(
      LEAD(product_category__record_loaded_at) OVER (PARTITION BY product_category__product_category_id ORDER BY product_category__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_category__record_valid_to,
    product_category__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_category__is_current_record,
    CASE
      WHEN product_category__is_current_record
      THEN product_category__record_loaded_at
      ELSE product_category__record_valid_to
    END AS product_category__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_category|adventure_works|',
      product_category__product_category_id,
      '~epoch|valid_from|',
      product_category__record_valid_from
    )::BLOB AS _pit_hook__product_category,
    CONCAT('product_category|adventure_works|', product_category__product_category_id)::BLOB AS _hook__product_category,
    *
  FROM validity
)
SELECT
  *
FROM hooks
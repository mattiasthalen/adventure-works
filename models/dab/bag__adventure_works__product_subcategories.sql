MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product_subcategory,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__product_subcategory, _hook__product_subcategory),
  references (_hook__product_category)
);

WITH staging AS (
  SELECT
    product_subcategory_id AS product_subcategory__product_subcategory_id,
    product_category_id AS product_subcategory__product_category_id,
    name AS product_subcategory__name,
    rowguid AS product_subcategory__rowguid,
    modified_date AS product_subcategory__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_subcategory__record_loaded_at
  FROM das.raw__adventure_works__product_subcategories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_subcategory__product_subcategory_id ORDER BY product_subcategory__record_loaded_at) AS product_subcategory__record_version,
    CASE
      WHEN product_subcategory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_subcategory__record_loaded_at
    END AS product_subcategory__record_valid_from,
    COALESCE(
      LEAD(product_subcategory__record_loaded_at) OVER (PARTITION BY product_subcategory__product_subcategory_id ORDER BY product_subcategory__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_subcategory__record_valid_to,
    product_subcategory__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_subcategory__is_current_record,
    CASE
      WHEN product_subcategory__is_current_record
      THEN product_subcategory__record_loaded_at
      ELSE product_subcategory__record_valid_to
    END AS product_subcategory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_subcategory__adventure_works|',
      product_subcategory__product_subcategory_id,
      '~epoch|valid_from|',
      product_subcategory__record_valid_from
    )::BLOB AS _pit_hook__product_subcategory,
    CONCAT('product_subcategory__adventure_works|', product_subcategory__product_subcategory_id) AS _hook__product_subcategory,
    CONCAT('product_category__adventure_works|', product_subcategory__product_category_id) AS _hook__product_category,
    *
  FROM validity
)
SELECT
  _pit_hook__product_subcategory::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__product_category::BLOB,
  product_subcategory__product_subcategory_id::BIGINT,
  product_subcategory__product_category_id::BIGINT,
  product_subcategory__name::TEXT,
  product_subcategory__rowguid::TEXT,
  product_subcategory__modified_date::DATE,
  product_subcategory__record_loaded_at::TIMESTAMP,
  product_subcategory__record_updated_at::TIMESTAMP,
  product_subcategory__record_version::TEXT,
  product_subcategory__record_valid_from::TIMESTAMP,
  product_subcategory__record_valid_to::TIMESTAMP,
  product_subcategory__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND product_subcategory__record_updated_at BETWEEN @start_ts AND @end_ts
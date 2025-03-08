MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_description,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__product_description, _hook__reference__product_description)
);

WITH staging AS (
  SELECT
    product_description_id AS product_description__product_description_id,
    description AS product_description__description,
    rowguid AS product_description__rowguid,
    modified_date AS product_description__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_description__record_loaded_at
  FROM das.raw__adventure_works__product_descriptions
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_description__product_description_id ORDER BY product_description__record_loaded_at) AS product_description__record_version,
    CASE
      WHEN product_description__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_description__record_loaded_at
    END AS product_description__record_valid_from,
    COALESCE(
      LEAD(product_description__record_loaded_at) OVER (PARTITION BY product_description__product_description_id ORDER BY product_description__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_description__record_valid_to,
    product_description__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_description__is_current_record,
    CASE
      WHEN product_description__is_current_record
      THEN product_description__record_loaded_at
      ELSE product_description__record_valid_to
    END AS product_description__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__product_description__adventure_works|',
      product_description__product_description_id,
      '~epoch|valid_from|',
      product_description__record_valid_from
    )::BLOB AS _pit_hook__reference__product_description,
    CONCAT('reference__product_description__adventure_works|', product_description__product_description_id) AS _hook__reference__product_description,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__product_description::BLOB,
  _hook__reference__product_description::BLOB,
  product_description__product_description_id::BIGINT,
  product_description__description::TEXT,
  product_description__rowguid::TEXT,
  product_description__modified_date::DATE,
  product_description__record_loaded_at::TIMESTAMP,
  product_description__record_updated_at::TIMESTAMP,
  product_description__record_version::TEXT,
  product_description__record_valid_from::TIMESTAMP,
  product_description__record_valid_to::TIMESTAMP,
  product_description__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_description__record_updated_at BETWEEN @start_ts AND @end_ts
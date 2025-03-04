MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_description__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_description_id AS product_description__product_description_id,
    description AS product_description__description,
    modified_date AS product_description__modified_date,
    rowguid AS product_description__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_description__record_loaded_at
  FROM bronze.raw__adventure_works__product_descriptions
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_description__product_description_id ORDER BY product_description__record_loaded_at) AS product_description__record_version,
    CASE
      WHEN product_description__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_description__record_loaded_at
    END AS product_description__record_valid_from,
    COALESCE(
      LEAD(product_description__record_loaded_at) OVER (PARTITION BY product_description__product_description_id ORDER BY product_description__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_description__record_valid_to,
    product_description__record_valid_to = @max_ts::TIMESTAMP AS product_description__is_current_record,
    CASE
      WHEN product_description__is_current_record
      THEN product_description__record_loaded_at
      ELSE product_description__record_valid_to
    END AS product_description__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product_description|adventure_works|',
      product_description__product_description_id,
      '~epoch|valid_from|',
      product_description__record_valid_from
    ) AS _pit_hook__product_description,
    CONCAT('product_description|adventure_works|', product_description__product_description_id) AS _hook__product_description,
    *
  FROM validity
)
SELECT
  _pit_hook__product_description::BLOB,
  _hook__product_description::BLOB,
  product_description__product_description_id::BIGINT,
  product_description__description::VARCHAR,
  product_description__modified_date::VARCHAR,
  product_description__rowguid::VARCHAR,
  product_description__record_loaded_at::TIMESTAMP,
  product_description__record_updated_at::TIMESTAMP,
  product_description__record_valid_from::TIMESTAMP,
  product_description__record_valid_to::TIMESTAMP,
  product_description__record_version::INT,
  product_description__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_description__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_inventori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    location_id AS product_inventori__location_id,
    product_id AS product_inventori__product_id,
    bin AS product_inventori__bin,
    modified_date AS product_inventori__modified_date,
    quantity AS product_inventori__quantity,
    rowguid AS product_inventori__rowguid,
    shelf AS product_inventori__shelf,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_inventori__record_loaded_at
  FROM bronze.raw__adventure_works__product_inventories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_inventori__location_id ORDER BY product_inventori__record_loaded_at) AS product_inventori__record_version,
    CASE
      WHEN product_inventori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_inventori__record_loaded_at
    END AS product_inventori__record_valid_from,
    COALESCE(
      LEAD(product_inventori__record_loaded_at) OVER (PARTITION BY product_inventori__location_id ORDER BY product_inventori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_inventori__record_valid_to,
    product_inventori__record_valid_to = @max_ts::TIMESTAMP AS product_inventori__is_current_record,
    CASE
      WHEN product_inventori__is_current_record
      THEN product_inventori__record_loaded_at
      ELSE product_inventori__record_valid_to
    END AS product_inventori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'location|adventure_works|',
      product_inventori__location_id,
      '~epoch|valid_from|',
      product_inventori__record_valid_from
    ) AS _pit_hook__location,
    CONCAT('location|adventure_works|', product_inventori__location_id) AS _hook__location,
    CONCAT('product|adventure_works|', product_inventori__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__location::BLOB,
  _hook__location::BLOB,
  _hook__product::BLOB,
  product_inventori__location_id::BIGINT,
  product_inventori__product_id::BIGINT,
  product_inventori__bin::BIGINT,
  product_inventori__modified_date::VARCHAR,
  product_inventori__quantity::BIGINT,
  product_inventori__rowguid::VARCHAR,
  product_inventori__shelf::VARCHAR,
  product_inventori__record_loaded_at::TIMESTAMP,
  product_inventori__record_updated_at::TIMESTAMP,
  product_inventori__record_valid_from::TIMESTAMP,
  product_inventori__record_valid_to::TIMESTAMP,
  product_inventori__record_version::INT,
  product_inventori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_inventori__record_updated_at BETWEEN @start_ts AND @end_ts
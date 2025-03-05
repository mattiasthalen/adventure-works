MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_inventory__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__reference__location, _hook__reference__location),
  references (_hook__product)
);

WITH staging AS (
  SELECT
    product_id AS product_inventory__product_id,
    location_id AS product_inventory__location_id,
    shelf AS product_inventory__shelf,
    bin AS product_inventory__bin,
    quantity AS product_inventory__quantity,
    rowguid AS product_inventory__rowguid,
    modified_date AS product_inventory__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_inventory__record_loaded_at
  FROM bronze.raw__adventure_works__product_inventories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_inventory__location_id ORDER BY product_inventory__record_loaded_at) AS product_inventory__record_version,
    CASE
      WHEN product_inventory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_inventory__record_loaded_at
    END AS product_inventory__record_valid_from,
    COALESCE(
      LEAD(product_inventory__record_loaded_at) OVER (PARTITION BY product_inventory__location_id ORDER BY product_inventory__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_inventory__record_valid_to,
    product_inventory__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_inventory__is_current_record,
    CASE
      WHEN product_inventory__is_current_record
      THEN product_inventory__record_loaded_at
      ELSE product_inventory__record_valid_to
    END AS product_inventory__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__location__adventure_works|',
      product_inventory__location_id,
      '~epoch|valid_from|',
      product_inventory__record_valid_from
    )::BLOB AS _pit_hook__reference__location,
    CONCAT('reference__location__adventure_works|', product_inventory__location_id) AS _hook__reference__location,
    CONCAT('product__adventure_works|', product_inventory__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__location::BLOB,
  _hook__reference__location::BLOB,
  _hook__product::BLOB,
  product_inventory__product_id::BIGINT,
  product_inventory__location_id::BIGINT,
  product_inventory__shelf::TEXT,
  product_inventory__bin::BIGINT,
  product_inventory__quantity::BIGINT,
  product_inventory__rowguid::UUID,
  product_inventory__modified_date::DATE,
  product_inventory__record_loaded_at::TIMESTAMP,
  product_inventory__record_updated_at::TIMESTAMP,
  product_inventory__record_version::TEXT,
  product_inventory__record_valid_from::TIMESTAMP,
  product_inventory__record_valid_to::TIMESTAMP,
  product_inventory__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND product_inventory__record_updated_at BETWEEN @start_ts AND @end_ts
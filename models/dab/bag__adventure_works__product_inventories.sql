MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_location
  ),
  tags hook,
  grain (_pit_hook__reference__product_location, _hook__reference__product_location),
  description 'Hook viewpoint of product_inventories data: Product inventory information.',
  references (_hook__reference__location, _hook__product),
  column_descriptions (
    product_inventory__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_inventory__location_id = 'Inventory location identification number. Foreign key to Location.LocationID.',
    product_inventory__shelf = 'Storage compartment within an inventory location.',
    product_inventory__bin = 'Storage container on a shelf in an inventory location.',
    product_inventory__quantity = 'Quantity of products in the inventory location.',
    product_inventory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_inventory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_inventory__record_updated_at = 'Timestamp when this record was last updated',
    product_inventory__record_version = 'Version number for this record',
    product_inventory__record_valid_from = 'Timestamp from which this record version is valid',
    product_inventory__record_valid_to = 'Timestamp until which this record version is valid',
    product_inventory__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__product_location = 'Reference hook to product_location reference',
    _hook__reference__location = 'Reference hook to location reference',
    _hook__product = 'Reference hook to product',
    _pit_hook__reference__product_location = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
  FROM das.raw__adventure_works__product_inventories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_inventory__location_id, product_inventory__product_id ORDER BY product_inventory__record_loaded_at) AS product_inventory__record_version,
    CASE
      WHEN product_inventory__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_inventory__record_loaded_at
    END AS product_inventory__record_valid_from,
    COALESCE(
      LEAD(product_inventory__record_loaded_at) OVER (PARTITION BY product_inventory__location_id, product_inventory__product_id ORDER BY product_inventory__record_loaded_at),
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
    CONCAT('reference__location__adventure_works|', product_inventory__location_id) AS _hook__reference__location,
    CONCAT('product__adventure_works|', product_inventory__product_id) AS _hook__product,
    CONCAT_WS('~', _hook__reference__location, _hook__product) AS _hook__reference__product_location,
    CONCAT_WS('~',
      _hook__reference__product_location,
      'epoch__valid_from|'||product_inventory__record_valid_from
    ) AS _pit_hook__reference__product_location,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__product_location::BLOB,
  _hook__reference__product_location::BLOB,
  _hook__reference__location::BLOB,
  _hook__product::BLOB,
  product_inventory__product_id::BIGINT,
  product_inventory__location_id::BIGINT,
  product_inventory__shelf::TEXT,
  product_inventory__bin::BIGINT,
  product_inventory__quantity::BIGINT,
  product_inventory__rowguid::TEXT,
  product_inventory__modified_date::DATE,
  product_inventory__record_loaded_at::TIMESTAMP,
  product_inventory__record_updated_at::TIMESTAMP,
  product_inventory__record_version::TEXT,
  product_inventory__record_valid_from::TIMESTAMP,
  product_inventory__record_valid_to::TIMESTAMP,
  product_inventory__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_inventory__record_updated_at BETWEEN @start_ts AND @end_ts
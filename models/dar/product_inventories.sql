MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__product_location),
  description 'Business viewpoint of product_inventories data: Product inventory information.',
  column_descriptions (
    product_inventory__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_inventory__location_id = 'Inventory location identification number. Foreign key to Location.LocationID.',
    product_inventory__shelf = 'Storage compartment within an inventory location.',
    product_inventory__bin = 'Storage container on a shelf in an inventory location.',
    product_inventory__quantity = 'Quantity of products in the inventory location.',
    product_inventory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_inventory__modified_date = 'Date when this record was last modified',
    product_inventory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_inventory__record_updated_at = 'Timestamp when this record was last updated',
    product_inventory__record_version = 'Version number for this record',
    product_inventory__record_valid_from = 'Timestamp from which this record version is valid',
    product_inventory__record_valid_to = 'Timestamp until which this record version is valid',
    product_inventory__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__product_location,
    product_inventory__product_id,
    product_inventory__location_id,
    product_inventory__shelf,
    product_inventory__bin,
    product_inventory__quantity,
    product_inventory__rowguid,
    product_inventory__modified_date,
    product_inventory__record_loaded_at,
    product_inventory__record_updated_at,
    product_inventory__record_version,
    product_inventory__record_valid_from,
    product_inventory__record_valid_to,
    product_inventory__is_current_record
  FROM dab.bag__adventure_works__product_inventories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__product_location,
    NULL AS product_inventory__product_id,
    NULL AS product_inventory__location_id,
    'N/A' AS product_inventory__shelf,
    NULL AS product_inventory__bin,
    NULL AS product_inventory__quantity,
    'N/A' AS product_inventory__rowguid,
    NULL AS product_inventory__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_inventory__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_inventory__record_updated_at,
    0 AS product_inventory__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_inventory__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_inventory__record_valid_to,
    TRUE AS product_inventory__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__product_location::BLOB,
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
  product_inventory__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_inventories TO './export/dar/product_inventories.parquet' (FORMAT parquet, COMPRESSION zstd)
);
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

SELECT
  *
  EXCLUDE (_hook__reference__product_location, _hook__reference__location, _hook__product)
FROM dab.bag__adventure_works__product_inventories
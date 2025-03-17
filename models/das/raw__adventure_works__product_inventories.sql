MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_inventories data: Product inventory information.',
  column_descriptions (
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    location_id = 'Inventory location identification number. Foreign key to Location.LocationID.',
    shelf = 'Storage compartment within an inventory location.',
    bin = 'Storage container on a shelf in an inventory location.',
    quantity = 'Quantity of products in the inventory location.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_id::BIGINT,
    location_id::BIGINT,
    shelf::TEXT,
    bin::BIGINT,
    quantity::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_inventories"
)
;
MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_subcategories data: Product subcategories. See ProductCategory table.',
  column_descriptions (
    product_subcategory_id = 'Primary key for ProductSubcategory records.',
    product_category_id = 'Product category identification number. Foreign key to ProductCategory.ProductCategoryID.',
    name = 'Subcategory description.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_subcategory_id::BIGINT,
    product_category_id::BIGINT,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_subcategories"
)
;
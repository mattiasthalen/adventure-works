MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_subcategory),
  description 'Business viewpoint of product_subcategories data: Product subcategories. See ProductCategory table.',
  column_descriptions (
    product_subcategory__product_subcategory_id = 'Primary key for ProductSubcategory records.',
    product_subcategory__product_category_id = 'Product category identification number. Foreign key to ProductCategory.ProductCategoryID.',
    product_subcategory__name = 'Subcategory description.',
    product_subcategory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_subcategory__modified_date = 'Date when this record was last modified',
    product_subcategory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_subcategory__record_updated_at = 'Timestamp when this record was last updated',
    product_subcategory__record_version = 'Version number for this record',
    product_subcategory__record_valid_from = 'Timestamp from which this record version is valid',
    product_subcategory__record_valid_to = 'Timestamp until which this record version is valid',
    product_subcategory__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__product_subcategory, _hook__product_category)
FROM dab.bag__adventure_works__product_subcategories
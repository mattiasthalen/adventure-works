MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_models data: Product model classification.',
  column_descriptions (
    product_model_id = 'Primary key for ProductModel records.',
    name = 'Product model description.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    catalog_description = 'Detailed product catalog information in xml format.',
    instructions = 'Manufacturing instructions in xml format.'
  )
);

SELECT
    product_model_id::BIGINT,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    catalog_description::TEXT,
    instructions::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_models"
)
;
MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_descriptions data: Product descriptions in several languages.',
  column_descriptions (
    product_description_id = 'Primary key for ProductDescription records.',
    description = 'Description of the product.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_description_id::BIGINT,
    description::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_descriptions"
)
;
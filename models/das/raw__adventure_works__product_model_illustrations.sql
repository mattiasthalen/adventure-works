MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_model_illustrations data: Cross-reference table mapping product models and illustrations.',
  column_descriptions (
    product_model_id = 'Primary key. Foreign key to ProductModel.ProductModelID.',
    illustration_id = 'Primary key. Foreign key to Illustration.IllustrationID.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_model_id::BIGINT,
    illustration_id::BIGINT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_model_illustrations"
)
;
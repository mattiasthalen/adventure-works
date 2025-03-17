MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_cost_histories data: Changes in the cost of a product over time.',
  column_descriptions (
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    start_date = 'Product cost start date.',
    end_date = 'Product cost end date.',
    standard_cost = 'Standard cost of the product.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_id::BIGINT,
    start_date::DATE,
    end_date::DATE,
    standard_cost::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_cost_histories"
)
;
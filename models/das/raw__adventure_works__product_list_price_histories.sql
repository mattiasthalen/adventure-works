MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_list_price_histories data: Changes in the list price of a product over time.',
  column_descriptions (
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    start_date = 'List price start date.',
    end_date = 'List price end date.',
    list_price = 'Product list price.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_id::BIGINT,
    start_date::DATE,
    end_date::DATE,
    list_price::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_list_price_histories"
)
;
MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  transaction_id::BIGINT,
  product_id::BIGINT,
  reference_order_id::BIGINT,
  reference_order_line_id::BIGINT,
  actual_cost::DOUBLE,
  modified_date::VARCHAR,
  quantity::BIGINT,
  transaction_date::VARCHAR,
  transaction_type::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__transaction_histories"
);

MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_id::BIGINT,
  end_date::VARCHAR,
  modified_date::VARCHAR,
  standard_cost::DOUBLE,
  start_date::TIMESTAMP,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_cost_histories"
);

MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  operation_sequence::BIGINT,
  location_id::BIGINT,
  product_id::BIGINT,
  work_order_id::BIGINT,
  actual_cost::DOUBLE,
  actual_end_date::VARCHAR,
  actual_resource_hrs::DOUBLE,
  actual_start_date::VARCHAR,
  modified_date::VARCHAR,
  planned_cost::DOUBLE,
  scheduled_end_date::VARCHAR,
  scheduled_start_date::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__work_order_routings"
);

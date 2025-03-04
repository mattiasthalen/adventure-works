MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  work_order_id::BIGINT,
  product_id::BIGINT,
  scrap_reason_id::BIGINT,
  due_date::VARCHAR,
  end_date::VARCHAR,
  modified_date::VARCHAR,
  order_qty::BIGINT,
  scrapped_qty::BIGINT,
  start_date::VARCHAR,
  stocked_qty::BIGINT,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__work_orders"
);

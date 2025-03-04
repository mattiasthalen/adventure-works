MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  purchase_order_detail_id::BIGINT,
  product_id::BIGINT,
  purchase_order_id::BIGINT,
  due_date::VARCHAR,
  line_total::DOUBLE,
  modified_date::VARCHAR,
  order_qty::BIGINT,
  received_qty::DOUBLE,
  rejected_qty::DOUBLE,
  stocked_qty::DOUBLE,
  unit_price::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__purchase_order_details"
);

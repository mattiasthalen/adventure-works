MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  product_id::BIGINT,
  average_lead_time::BIGINT,
  last_receipt_cost::DOUBLE,
  last_receipt_date::VARCHAR,
  max_order_qty::BIGINT,
  min_order_qty::BIGINT,
  modified_date::VARCHAR,
  on_order_qty::BIGINT,
  standard_price::DOUBLE,
  unit_measure_code::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_vendors"
);

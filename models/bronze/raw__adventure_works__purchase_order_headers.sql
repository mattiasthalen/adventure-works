MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  purchase_order_id::BIGINT,
  employee_id::BIGINT,
  ship_method_id::BIGINT,
  vendor_id::BIGINT,
  freight::DOUBLE,
  modified_date::VARCHAR,
  order_date::VARCHAR,
  revision_number::BIGINT,
  ship_date::VARCHAR,
  status::BIGINT,
  sub_total::DOUBLE,
  tax_amt::DOUBLE,
  total_due::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__purchase_order_headers"
);

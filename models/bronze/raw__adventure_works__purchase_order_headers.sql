MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  purchase_order_id,
  employee_id,
  freight,
  modified_date,
  order_date,
  revision_number,
  ship_date,
  ship_method_id,
  status,
  sub_total,
  tax_amt,
  total_due,
  vendor_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__purchase_order_headers"
)

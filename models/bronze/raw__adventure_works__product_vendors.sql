MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  average_lead_time,
  last_receipt_cost,
  last_receipt_date,
  max_order_qty,
  min_order_qty,
  modified_date,
  on_order_qty,
  product_id,
  standard_price,
  unit_measure_code,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_vendors"
)

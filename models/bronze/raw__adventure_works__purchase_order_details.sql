MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  purchase_order_detail_id,
  due_date,
  line_total,
  modified_date,
  order_qty,
  product_id,
  purchase_order_id,
  received_qty,
  rejected_qty,
  stocked_qty,
  unit_price,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__purchase_order_details"
)

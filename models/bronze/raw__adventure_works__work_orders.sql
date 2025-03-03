MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  work_order_id,
  due_date,
  end_date,
  modified_date,
  order_qty,
  product_id,
  scrap_reason_id,
  scrapped_qty,
  start_date,
  stocked_qty,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__work_orders"
)

MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  operation_sequence,
  actual_cost,
  actual_end_date,
  actual_resource_hrs,
  actual_start_date,
  location_id,
  modified_date,
  planned_cost,
  product_id,
  scheduled_end_date,
  scheduled_start_date,
  work_order_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__work_order_routings"
)

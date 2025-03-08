MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    work_order_id::BIGINT,
    product_id::BIGINT,
    operation_sequence::BIGINT,
    location_id::BIGINT,
    scheduled_start_date::DATE,
    scheduled_end_date::DATE,
    actual_start_date::DATE,
    actual_end_date::DATE,
    actual_resource_hrs::DOUBLE,
    planned_cost::DOUBLE,
    actual_cost::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__work_order_routings"
)
;
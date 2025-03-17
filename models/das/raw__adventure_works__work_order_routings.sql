MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of work_order_routings data: Work order details.',
  column_descriptions (
    work_order_id = 'Primary key. Foreign key to WorkOrder.WorkOrderID.',
    product_id = 'Primary key. Foreign key to Product.ProductID.',
    operation_sequence = 'Primary key. Indicates the manufacturing process sequence.',
    location_id = 'Manufacturing location where the part is processed. Foreign key to Location.LocationID.',
    scheduled_start_date = 'Planned manufacturing start date.',
    scheduled_end_date = 'Planned manufacturing end date.',
    actual_start_date = 'Actual start date.',
    actual_end_date = 'Actual end date.',
    actual_resource_hrs = 'Number of manufacturing hours used.',
    planned_cost = 'Estimated manufacturing cost.',
    actual_cost = 'Actual manufacturing cost.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
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
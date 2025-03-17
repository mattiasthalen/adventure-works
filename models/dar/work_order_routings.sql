MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__work_order_routing),
  description 'Business viewpoint of work_order_routings data: Work order details.',
  column_descriptions (
    work_order_routing__work_order_id = 'Primary key. Foreign key to WorkOrder.WorkOrderID.',
    work_order_routing__product_id = 'Primary key. Foreign key to Product.ProductID.',
    work_order_routing__operation_sequence = 'Primary key. Indicates the manufacturing process sequence.',
    work_order_routing__location_id = 'Manufacturing location where the part is processed. Foreign key to Location.LocationID.',
    work_order_routing__scheduled_start_date = 'Planned manufacturing start date.',
    work_order_routing__scheduled_end_date = 'Planned manufacturing end date.',
    work_order_routing__actual_start_date = 'Actual start date.',
    work_order_routing__actual_end_date = 'Actual end date.',
    work_order_routing__actual_resource_hrs = 'Number of manufacturing hours used.',
    work_order_routing__planned_cost = 'Estimated manufacturing cost.',
    work_order_routing__actual_cost = 'Actual manufacturing cost.',
    work_order_routing__modified_date = 'Date when this record was last modified',
    work_order_routing__record_loaded_at = 'Timestamp when this record was loaded into the system',
    work_order_routing__record_updated_at = 'Timestamp when this record was last updated',
    work_order_routing__record_version = 'Version number for this record',
    work_order_routing__record_valid_from = 'Timestamp from which this record version is valid',
    work_order_routing__record_valid_to = 'Timestamp until which this record version is valid',
    work_order_routing__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__work_order_routing, _hook__order_line__work, _hook__order__work, _hook__product, _hook__reference__location)
FROM dab.bag__adventure_works__work_order_routings
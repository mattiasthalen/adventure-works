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

WITH cte__source AS (
  SELECT
    _pit_hook__work_order_routing,
    work_order_routing__work_order_id,
    work_order_routing__product_id,
    work_order_routing__operation_sequence,
    work_order_routing__location_id,
    work_order_routing__scheduled_start_date,
    work_order_routing__scheduled_end_date,
    work_order_routing__actual_start_date,
    work_order_routing__actual_end_date,
    work_order_routing__actual_resource_hrs,
    work_order_routing__planned_cost,
    work_order_routing__actual_cost,
    work_order_routing__modified_date,
    work_order_routing__record_loaded_at,
    work_order_routing__record_updated_at,
    work_order_routing__record_version,
    work_order_routing__record_valid_from,
    work_order_routing__record_valid_to,
    work_order_routing__is_current_record
  FROM dab.bag__adventure_works__work_order_routings
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__work_order_routing,
    NULL AS work_order_routing__work_order_id,
    NULL AS work_order_routing__product_id,
    NULL AS work_order_routing__operation_sequence,
    NULL AS work_order_routing__location_id,
    NULL AS work_order_routing__scheduled_start_date,
    NULL AS work_order_routing__scheduled_end_date,
    NULL AS work_order_routing__actual_start_date,
    NULL AS work_order_routing__actual_end_date,
    NULL AS work_order_routing__actual_resource_hrs,
    NULL AS work_order_routing__planned_cost,
    NULL AS work_order_routing__actual_cost,
    NULL AS work_order_routing__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order_routing__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order_routing__record_updated_at,
    0 AS work_order_routing__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order_routing__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS work_order_routing__record_valid_to,
    TRUE AS work_order_routing__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__work_order_routing::BLOB,
  work_order_routing__work_order_id::BIGINT,
  work_order_routing__product_id::BIGINT,
  work_order_routing__operation_sequence::BIGINT,
  work_order_routing__location_id::BIGINT,
  work_order_routing__scheduled_start_date::DATE,
  work_order_routing__scheduled_end_date::DATE,
  work_order_routing__actual_start_date::DATE,
  work_order_routing__actual_end_date::DATE,
  work_order_routing__actual_resource_hrs::DOUBLE,
  work_order_routing__planned_cost::DOUBLE,
  work_order_routing__actual_cost::DOUBLE,
  work_order_routing__modified_date::DATE,
  work_order_routing__record_loaded_at::TIMESTAMP,
  work_order_routing__record_updated_at::TIMESTAMP,
  work_order_routing__record_version::TEXT,
  work_order_routing__record_valid_from::TIMESTAMP,
  work_order_routing__record_valid_to::TIMESTAMP,
  work_order_routing__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.work_order_routings TO './export/dar/work_order_routings.parquet' (FORMAT parquet, COMPRESSION zstd)
);
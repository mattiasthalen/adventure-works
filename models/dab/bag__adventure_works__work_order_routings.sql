MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__work_order_routing
  ),
  tags hook,
  grain (_pit_hook__work_order_routing, _hook__work_order_routing),
  description 'Hook viewpoint of work_order_routings data: Work order details.',
  references (_hook__order_line__work, _hook__order__work, _hook__product, _hook__reference__location),
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
    work_order_routing__record_loaded_at = 'Timestamp when this record was loaded into the system',
    work_order_routing__record_updated_at = 'Timestamp when this record was last updated',
    work_order_routing__record_version = 'Version number for this record',
    work_order_routing__record_valid_from = 'Timestamp from which this record version is valid',
    work_order_routing__record_valid_to = 'Timestamp until which this record version is valid',
    work_order_routing__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__work_order_routing = 'Reference hook to work_order_routing',
    _hook__order_line__work = 'Reference hook to work order_line',
    _hook__order__work = 'Reference hook to work order',
    _hook__product = 'Reference hook to product',
    _hook__reference__location = 'Reference hook to location reference',
    _pit_hook__work_order_routing = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    work_order_id AS work_order_routing__work_order_id,
    product_id AS work_order_routing__product_id,
    operation_sequence AS work_order_routing__operation_sequence,
    location_id AS work_order_routing__location_id,
    scheduled_start_date AS work_order_routing__scheduled_start_date,
    scheduled_end_date AS work_order_routing__scheduled_end_date,
    actual_start_date AS work_order_routing__actual_start_date,
    actual_end_date AS work_order_routing__actual_end_date,
    actual_resource_hrs AS work_order_routing__actual_resource_hrs,
    planned_cost AS work_order_routing__planned_cost,
    actual_cost AS work_order_routing__actual_cost,
    modified_date AS work_order_routing__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS work_order_routing__record_loaded_at
  FROM das.raw__adventure_works__work_order_routings
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_order_routing__operation_sequence, work_order_routing__product_id, work_order_routing__work_order_id ORDER BY work_order_routing__record_loaded_at) AS work_order_routing__record_version,
    CASE
      WHEN work_order_routing__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE work_order_routing__record_loaded_at
    END AS work_order_routing__record_valid_from,
    COALESCE(
      LEAD(work_order_routing__record_loaded_at) OVER (PARTITION BY work_order_routing__operation_sequence, work_order_routing__product_id, work_order_routing__work_order_id ORDER BY work_order_routing__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS work_order_routing__record_valid_to,
    work_order_routing__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS work_order_routing__is_current_record,
    CASE
      WHEN work_order_routing__is_current_record
      THEN work_order_routing__record_loaded_at
      ELSE work_order_routing__record_valid_to
    END AS work_order_routing__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('order_line__work__adventure_works|', work_order_routing__operation_sequence) AS _hook__order_line__work,
    CONCAT('order__work__adventure_works|', work_order_routing__work_order_id) AS _hook__order__work,
    CONCAT('product__adventure_works|', work_order_routing__product_id) AS _hook__product,
    CONCAT('reference__location__adventure_works|', work_order_routing__location_id) AS _hook__reference__location,
    CONCAT_WS('~', _hook__order_line__work, _hook__product, _hook__order__work) AS _hook__work_order_routing,
    CONCAT_WS('~',
      _hook__work_order_routing,
      'epoch__valid_from|'||work_order_routing__record_valid_from
    ) AS _pit_hook__work_order_routing,
    *
  FROM validity
)
SELECT
  _pit_hook__work_order_routing::BLOB,
  _hook__work_order_routing::BLOB,
  _hook__order_line__work::BLOB,
  _hook__order__work::BLOB,
  _hook__product::BLOB,
  _hook__reference__location::BLOB,
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
  work_order_routing__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND work_order_routing__record_updated_at BETWEEN @start_ts AND @end_ts
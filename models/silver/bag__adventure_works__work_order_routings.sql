MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column work_order_routing__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__order_line__work, _hook__order_line__work),
  references (_hook__order__work, _hook__product, _hook__reference__location)
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
  FROM bronze.raw__adventure_works__work_order_routings
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_order_routing__operation_sequence ORDER BY work_order_routing__record_loaded_at) AS work_order_routing__record_version,
    CASE
      WHEN work_order_routing__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE work_order_routing__record_loaded_at
    END AS work_order_routing__record_valid_from,
    COALESCE(
      LEAD(work_order_routing__record_loaded_at) OVER (PARTITION BY work_order_routing__operation_sequence ORDER BY work_order_routing__record_loaded_at),
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
    CONCAT(
      'order_line__work__adventure_works|',
      work_order_routing__operation_sequence,
      '~epoch|valid_from|',
      work_order_routing__record_valid_from
    )::BLOB AS _pit_hook__order_line__work,
    CONCAT('order_line__work__adventure_works|', work_order_routing__operation_sequence) AS _hook__order_line__work,
    CONCAT('order__work__adventure_works|', work_order_routing__work_order_id) AS _hook__order__work,
    CONCAT('product__adventure_works|', work_order_routing__product_id) AS _hook__product,
    CONCAT('reference__location__adventure_works|', work_order_routing__location_id) AS _hook__reference__location,
    *
  FROM validity
)
SELECT
  _pit_hook__order_line__work::BLOB,
  _hook__order_line__work::BLOB,
  _hook__order__work::BLOB,
  _hook__product::BLOB,
  _hook__reference__location::BLOB,
  work_order_routing__work_order_id::BIGINT,
  work_order_routing__product_id::BIGINT,
  work_order_routing__operation_sequence::BIGINT,
  work_order_routing__location_id::BIGINT,
  work_order_routing__scheduled_start_date::TEXT,
  work_order_routing__scheduled_end_date::TEXT,
  work_order_routing__actual_start_date::TEXT,
  work_order_routing__actual_end_date::TEXT,
  work_order_routing__actual_resource_hrs::DOUBLE,
  work_order_routing__planned_cost::DOUBLE,
  work_order_routing__actual_cost::DOUBLE,
  work_order_routing__modified_date::DATE,
  work_order_routing__record_loaded_at::TIMESTAMP,
  work_order_routing__record_updated_at::TIMESTAMP,
  work_order_routing__record_version::TEXT,
  work_order_routing__record_valid_from::TIMESTAMP,
  work_order_routing__record_valid_to::TIMESTAMP,
  work_order_routing__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND work_order_routing__record_updated_at BETWEEN @start_ts AND @end_ts
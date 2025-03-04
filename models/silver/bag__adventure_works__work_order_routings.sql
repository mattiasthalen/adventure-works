MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column work_order_routing__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    operation_sequence AS work_order_routing__operation_sequence,
    location_id AS work_order_routing__location_id,
    product_id AS work_order_routing__product_id,
    work_order_id AS work_order_routing__work_order_id,
    actual_cost AS work_order_routing__actual_cost,
    actual_end_date AS work_order_routing__actual_end_date,
    actual_resource_hrs AS work_order_routing__actual_resource_hrs,
    actual_start_date AS work_order_routing__actual_start_date,
    modified_date AS work_order_routing__modified_date,
    planned_cost AS work_order_routing__planned_cost,
    scheduled_end_date AS work_order_routing__scheduled_end_date,
    scheduled_start_date AS work_order_routing__scheduled_start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS work_order_routing__record_loaded_at
  FROM bronze.raw__adventure_works__work_order_routings
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_order_routing__operation_sequence ORDER BY work_order_routing__record_loaded_at) AS work_order_routing__record_version,
    CASE
      WHEN work_order_routing__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE work_order_routing__record_loaded_at
    END AS work_order_routing__record_valid_from,
    COALESCE(
      LEAD(work_order_routing__record_loaded_at) OVER (PARTITION BY work_order_routing__operation_sequence ORDER BY work_order_routing__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS work_order_routing__record_valid_to,
    work_order_routing__record_valid_to = @max_ts::TIMESTAMP AS work_order_routing__is_current_record,
    CASE
      WHEN work_order_routing__is_current_record
      THEN work_order_routing__record_loaded_at
      ELSE work_order_routing__record_valid_to
    END AS work_order_routing__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'operation_sequence|adventure_works|',
      work_order_routing__operation_sequence,
      '~epoch|valid_from|',
      work_order_routing__record_valid_from
    ) AS _pit_hook__operation_sequence,
    CONCAT('operation_sequence|adventure_works|', work_order_routing__operation_sequence) AS _hook__operation_sequence,
    CONCAT('work_order|adventure_works|', work_order_routing__work_order_id) AS _hook__work_order,
    CONCAT('product|adventure_works|', work_order_routing__product_id) AS _hook__product,
    CONCAT('location|adventure_works|', work_order_routing__location_id) AS _hook__location,
    *
  FROM validity
)
SELECT
  _pit_hook__operation_sequence::BLOB,
  _hook__operation_sequence::BLOB,
  _hook__location::BLOB,
  _hook__product::BLOB,
  _hook__work_order::BLOB,
  work_order_routing__operation_sequence::BIGINT,
  work_order_routing__location_id::BIGINT,
  work_order_routing__product_id::BIGINT,
  work_order_routing__work_order_id::BIGINT,
  work_order_routing__actual_cost::DOUBLE,
  work_order_routing__actual_end_date::VARCHAR,
  work_order_routing__actual_resource_hrs::DOUBLE,
  work_order_routing__actual_start_date::VARCHAR,
  work_order_routing__modified_date::VARCHAR,
  work_order_routing__planned_cost::DOUBLE,
  work_order_routing__scheduled_end_date::VARCHAR,
  work_order_routing__scheduled_start_date::VARCHAR,
  work_order_routing__record_loaded_at::TIMESTAMP,
  work_order_routing__record_updated_at::TIMESTAMP,
  work_order_routing__record_valid_from::TIMESTAMP,
  work_order_routing__record_valid_to::TIMESTAMP,
  work_order_routing__record_version::INT,
  work_order_routing__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND work_order_routing__record_updated_at BETWEEN @start_ts AND @end_ts
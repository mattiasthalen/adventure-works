MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    operation_sequence AS operation_sequence__operation_sequence,
    location_id AS operation_sequence__location_id,
    product_id AS operation_sequence__product_id,
    work_order_id AS operation_sequence__work_order_id,
    actual_cost AS operation_sequence__actual_cost,
    actual_end_date AS operation_sequence__actual_end_date,
    actual_resource_hrs AS operation_sequence__actual_resource_hrs,
    actual_start_date AS operation_sequence__actual_start_date,
    modified_date AS operation_sequence__modified_date,
    planned_cost AS operation_sequence__planned_cost,
    scheduled_end_date AS operation_sequence__scheduled_end_date,
    scheduled_start_date AS operation_sequence__scheduled_start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS operation_sequence__record_loaded_at
  FROM bronze.raw__adventure_works__work_order_routings
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY operation_sequence__operation_sequence ORDER BY operation_sequence__record_loaded_at) AS operation_sequence__record_version,
    CASE
      WHEN operation_sequence__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE operation_sequence__record_loaded_at
    END AS operation_sequence__record_valid_from,
    COALESCE(
      LEAD(operation_sequence__record_loaded_at) OVER (PARTITION BY operation_sequence__operation_sequence ORDER BY operation_sequence__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS operation_sequence__record_valid_to,
    operation_sequence__record_valid_to = @max_ts::TIMESTAMP AS operation_sequence__is_current_record,
    CASE
      WHEN operation_sequence__is_current_record
      THEN operation_sequence__record_loaded_at
      ELSE operation_sequence__record_valid_to
    END AS operation_sequence__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'operation_sequence|adventure_works|',
      operation_sequence__operation_sequence,
      '~epoch|valid_from|',
      operation_sequence__record_valid_from
    ) AS _pit_hook__operation_sequence,
    CONCAT('operation_sequence|adventure_works|', operation_sequence__operation_sequence) AS _hook__operation_sequence,
    CONCAT('location|adventure_works|', operation_sequence__location_id) AS _hook__location,
    CONCAT('product|adventure_works|', operation_sequence__product_id) AS _hook__product,
    CONCAT('work_order|adventure_works|', operation_sequence__work_order_id) AS _hook__work_order,
    *
  FROM validity
)
SELECT
  _pit_hook__operation_sequence::BLOB,
  _hook__operation_sequence::BLOB,
  _hook__location::BLOB,
  _hook__product::BLOB,
  _hook__work_order::BLOB,
  operation_sequence__operation_sequence::VARCHAR,
  operation_sequence__location_id::VARCHAR,
  operation_sequence__product_id::VARCHAR,
  operation_sequence__work_order_id::VARCHAR,
  operation_sequence__actual_cost::VARCHAR,
  operation_sequence__actual_end_date::VARCHAR,
  operation_sequence__actual_resource_hrs::VARCHAR,
  operation_sequence__actual_start_date::VARCHAR,
  operation_sequence__modified_date::VARCHAR,
  operation_sequence__planned_cost::VARCHAR,
  operation_sequence__scheduled_end_date::VARCHAR,
  operation_sequence__scheduled_start_date::VARCHAR,
  operation_sequence__record_loaded_at::TIMESTAMP,
  operation_sequence__record_version::INT,
  operation_sequence__record_valid_from::TIMESTAMP,
  operation_sequence__record_valid_to::TIMESTAMP,
  operation_sequence__is_current_record::BOOLEAN,
  operation_sequence__record_updated_at::TIMESTAMP
FROM hooks
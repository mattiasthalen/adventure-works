MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column work_order__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    work_order_id AS work_order__work_order_id,
    product_id AS work_order__product_id,
    scrap_reason_id AS work_order__scrap_reason_id,
    due_date AS work_order__due_date,
    end_date AS work_order__end_date,
    modified_date AS work_order__modified_date,
    order_qty AS work_order__order_qty,
    scrapped_qty AS work_order__scrapped_qty,
    start_date AS work_order__start_date,
    stocked_qty AS work_order__stocked_qty,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS work_order__record_loaded_at
  FROM bronze.raw__adventure_works__work_orders
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_order__work_order_id ORDER BY work_order__record_loaded_at) AS work_order__record_version,
    CASE
      WHEN work_order__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE work_order__record_loaded_at
    END AS work_order__record_valid_from,
    COALESCE(
      LEAD(work_order__record_loaded_at) OVER (PARTITION BY work_order__work_order_id ORDER BY work_order__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS work_order__record_valid_to,
    work_order__record_valid_to = @max_ts::TIMESTAMP AS work_order__is_current_record,
    CASE
      WHEN work_order__is_current_record
      THEN work_order__record_loaded_at
      ELSE work_order__record_valid_to
    END AS work_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'work_order|adventure_works|',
      work_order__work_order_id,
      '~epoch|valid_from|',
      work_order__record_valid_from
    ) AS _pit_hook__work_order,
    CONCAT('work_order|adventure_works|', work_order__work_order_id) AS _hook__work_order,
    CONCAT('product|adventure_works|', work_order__product_id) AS _hook__product,
    CONCAT('scrap_reason|adventure_works|', work_order__scrap_reason_id) AS _hook__scrap_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__work_order::BLOB,
  _hook__work_order::BLOB,
  _hook__product::BLOB,
  _hook__scrap_reason::BLOB,
  work_order__work_order_id::BIGINT,
  work_order__product_id::BIGINT,
  work_order__scrap_reason_id::BIGINT,
  work_order__due_date::VARCHAR,
  work_order__end_date::VARCHAR,
  work_order__modified_date::VARCHAR,
  work_order__order_qty::BIGINT,
  work_order__scrapped_qty::BIGINT,
  work_order__start_date::VARCHAR,
  work_order__stocked_qty::BIGINT,
  work_order__record_loaded_at::TIMESTAMP,
  work_order__record_updated_at::TIMESTAMP,
  work_order__record_valid_from::TIMESTAMP,
  work_order__record_valid_to::TIMESTAMP,
  work_order__record_version::INT,
  work_order__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND work_order__record_updated_at BETWEEN @start_ts AND @end_ts
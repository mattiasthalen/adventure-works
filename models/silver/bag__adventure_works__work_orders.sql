MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column work_order__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__order__work, _hook__order__work),
  references (_hook__product, _hook__reference__scrap_reason)
);

WITH staging AS (
  SELECT
    work_order_id AS work_order__work_order_id,
    product_id AS work_order__product_id,
    order_qty AS work_order__order_qty,
    stocked_qty AS work_order__stocked_qty,
    scrapped_qty AS work_order__scrapped_qty,
    start_date AS work_order__start_date,
    end_date AS work_order__end_date,
    due_date AS work_order__due_date,
    scrap_reason_id AS work_order__scrap_reason_id,
    modified_date AS work_order__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS work_order__record_loaded_at
  FROM bronze.raw__adventure_works__work_orders
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY work_order__work_order_id ORDER BY work_order__record_loaded_at) AS work_order__record_version,
    CASE
      WHEN work_order__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE work_order__record_loaded_at
    END AS work_order__record_valid_from,
    COALESCE(
      LEAD(work_order__record_loaded_at) OVER (PARTITION BY work_order__work_order_id ORDER BY work_order__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS work_order__record_valid_to,
    work_order__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS work_order__is_current_record,
    CASE
      WHEN work_order__is_current_record
      THEN work_order__record_loaded_at
      ELSE work_order__record_valid_to
    END AS work_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'order__work__adventure_works|',
      work_order__work_order_id,
      '~epoch|valid_from|',
      work_order__record_valid_from
    )::BLOB AS _pit_hook__order__work,
    CONCAT('order__work__adventure_works|', work_order__work_order_id) AS _hook__order__work,
    CONCAT('product__adventure_works|', work_order__product_id) AS _hook__product,
    CONCAT('reference__scrap_reason__adventure_works|', work_order__scrap_reason_id) AS _hook__reference__scrap_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__order__work::BLOB,
  _hook__order__work::BLOB,
  _hook__product::BLOB,
  _hook__reference__scrap_reason::BLOB,
  work_order__work_order_id::BIGINT,
  work_order__product_id::BIGINT,
  work_order__order_qty::BIGINT,
  work_order__stocked_qty::BIGINT,
  work_order__scrapped_qty::BIGINT,
  work_order__start_date::TEXT,
  work_order__end_date::TEXT,
  work_order__due_date::TEXT,
  work_order__scrap_reason_id::BIGINT,
  work_order__modified_date::DATE,
  work_order__record_loaded_at::TIMESTAMP,
  work_order__record_updated_at::TIMESTAMP,
  work_order__record_version::TEXT,
  work_order__record_valid_from::TIMESTAMP,
  work_order__record_valid_to::TIMESTAMP,
  work_order__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND work_order__record_updated_at BETWEEN @start_ts AND @end_ts
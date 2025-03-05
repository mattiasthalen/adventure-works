MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column purchase_order_detail__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__order_line__purchase, _hook__order_line__purchase),
  references (_hook__order__purchase, _hook__product)
);

WITH staging AS (
  SELECT
    purchase_order_id AS purchase_order_detail__purchase_order_id,
    purchase_order_detail_id AS purchase_order_detail__purchase_order_detail_id,
    due_date AS purchase_order_detail__due_date,
    order_qty AS purchase_order_detail__order_qty,
    product_id AS purchase_order_detail__product_id,
    unit_price AS purchase_order_detail__unit_price,
    line_total AS purchase_order_detail__line_total,
    received_qty AS purchase_order_detail__received_qty,
    rejected_qty AS purchase_order_detail__rejected_qty,
    stocked_qty AS purchase_order_detail__stocked_qty,
    modified_date AS purchase_order_detail__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order_detail__record_loaded_at
  FROM bronze.raw__adventure_works__purchase_order_details
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order_detail__purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at) AS purchase_order_detail__record_version,
    CASE
      WHEN purchase_order_detail__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE purchase_order_detail__record_loaded_at
    END AS purchase_order_detail__record_valid_from,
    COALESCE(
      LEAD(purchase_order_detail__record_loaded_at) OVER (PARTITION BY purchase_order_detail__purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS purchase_order_detail__record_valid_to,
    purchase_order_detail__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS purchase_order_detail__is_current_record,
    CASE
      WHEN purchase_order_detail__is_current_record
      THEN purchase_order_detail__record_loaded_at
      ELSE purchase_order_detail__record_valid_to
    END AS purchase_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'order_line__purchase__adventure_works|',
      purchase_order_detail__purchase_order_detail_id,
      '~epoch__valid_from|',
      purchase_order_detail__record_valid_from
    )::BLOB AS _pit_hook__order_line__purchase,
    CONCAT('order_line__purchase__adventure_works|', purchase_order_detail__purchase_order_detail_id) AS _hook__order_line__purchase,
    CONCAT('order__purchase__adventure_works|', purchase_order_detail__purchase_order_id) AS _hook__order__purchase,
    CONCAT('product__adventure_works|', purchase_order_detail__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__order_line__purchase::BLOB,
  _hook__order_line__purchase::BLOB,
  _hook__order__purchase::BLOB,
  _hook__product::BLOB,
  purchase_order_detail__purchase_order_id::BIGINT,
  purchase_order_detail__purchase_order_detail_id::BIGINT,
  purchase_order_detail__due_date::TEXT,
  purchase_order_detail__order_qty::BIGINT,
  purchase_order_detail__product_id::BIGINT,
  purchase_order_detail__unit_price::DOUBLE,
  purchase_order_detail__line_total::DOUBLE,
  purchase_order_detail__received_qty::DOUBLE,
  purchase_order_detail__rejected_qty::DOUBLE,
  purchase_order_detail__stocked_qty::DOUBLE,
  purchase_order_detail__modified_date::DATE,
  purchase_order_detail__record_loaded_at::TIMESTAMP,
  purchase_order_detail__record_updated_at::TIMESTAMP,
  purchase_order_detail__record_version::TEXT,
  purchase_order_detail__record_valid_from::TIMESTAMP,
  purchase_order_detail__record_valid_to::TIMESTAMP,
  purchase_order_detail__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND purchase_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts
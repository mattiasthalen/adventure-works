MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    purchase_order_detail_id AS purchase_order_detail__purchase_order_detail_id,
    product_id AS purchase_order_detail__product_id,
    purchase_order_id AS purchase_order_detail__purchase_order_id,
    due_date AS purchase_order_detail__due_date,
    line_total AS purchase_order_detail__line_total,
    modified_date AS purchase_order_detail__modified_date,
    order_qty AS purchase_order_detail__order_qty,
    received_qty AS purchase_order_detail__received_qty,
    rejected_qty AS purchase_order_detail__rejected_qty,
    stocked_qty AS purchase_order_detail__stocked_qty,
    unit_price AS purchase_order_detail__unit_price,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order_detail__record_loaded_at
  FROM bronze.raw__adventure_works__purchase_order_details
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order_detail__purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at) AS purchase_order_detail__record_version,
    CASE
      WHEN purchase_order_detail__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE purchase_order_detail__record_loaded_at
    END AS purchase_order_detail__record_valid_from,
    COALESCE(
      LEAD(purchase_order_detail__record_loaded_at) OVER (PARTITION BY purchase_order_detail__purchase_order_detail_id ORDER BY purchase_order_detail__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS purchase_order_detail__record_valid_to,
    purchase_order_detail__record_valid_to = @max_ts::TIMESTAMP AS purchase_order_detail__is_current_record,
    CASE
      WHEN purchase_order_detail__is_current_record
      THEN purchase_order_detail__record_loaded_at
      ELSE purchase_order_detail__record_valid_to
    END AS purchase_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'purchase_order_detail|adventure_works|',
      purchase_order_detail__purchase_order_detail_id,
      '~epoch|valid_from|',
      purchase_order_detail__record_valid_from
    ) AS _pit_hook__purchase_order_detail,
    CONCAT('purchase_order_detail|adventure_works|', purchase_order_detail__purchase_order_detail_id) AS _hook__purchase_order_detail,
    CONCAT('product|adventure_works|', purchase_order_detail__product_id) AS _hook__product,
    CONCAT('purchase_order|adventure_works|', purchase_order_detail__purchase_order_id) AS _hook__purchase_order,
    *
  FROM validity
)
SELECT
  _pit_hook__purchase_order_detail::BLOB,
  _hook__purchase_order_detail::BLOB,
  _hook__product::BLOB,
  _hook__purchase_order::BLOB,
  purchase_order_detail__purchase_order_detail_id::VARCHAR,
  purchase_order_detail__product_id::VARCHAR,
  purchase_order_detail__purchase_order_id::VARCHAR,
  purchase_order_detail__due_date::VARCHAR,
  purchase_order_detail__line_total::VARCHAR,
  purchase_order_detail__modified_date::VARCHAR,
  purchase_order_detail__order_qty::VARCHAR,
  purchase_order_detail__received_qty::VARCHAR,
  purchase_order_detail__rejected_qty::VARCHAR,
  purchase_order_detail__stocked_qty::VARCHAR,
  purchase_order_detail__unit_price::VARCHAR,
  purchase_order_detail__record_loaded_at::TIMESTAMP,
  purchase_order_detail__record_version::INT,
  purchase_order_detail__record_valid_from::TIMESTAMP,
  purchase_order_detail__record_valid_to::TIMESTAMP,
  purchase_order_detail__is_current_record::BOOLEAN,
  purchase_order_detail__record_updated_at::TIMESTAMP
FROM hooks
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_order_detail__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    sales_order_detail_id AS sales_order_detail__sales_order_detail_id,
    product_id AS sales_order_detail__product_id,
    sales_order_id AS sales_order_detail__sales_order_id,
    special_offer_id AS sales_order_detail__special_offer_id,
    carrier_tracking_number AS sales_order_detail__carrier_tracking_number,
    line_total AS sales_order_detail__line_total,
    modified_date AS sales_order_detail__modified_date,
    order_qty AS sales_order_detail__order_qty,
    rowguid AS sales_order_detail__rowguid,
    unit_price AS sales_order_detail__unit_price,
    unit_price_discount AS sales_order_detail__unit_price_discount,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order_detail__record_loaded_at
  FROM bronze.raw__adventure_works__sales_order_details
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_detail__sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at) AS sales_order_detail__record_version,
    CASE
      WHEN sales_order_detail__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_order_detail__record_loaded_at
    END AS sales_order_detail__record_valid_from,
    COALESCE(
      LEAD(sales_order_detail__record_loaded_at) OVER (PARTITION BY sales_order_detail__sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_order_detail__record_valid_to,
    sales_order_detail__record_valid_to = @max_ts::TIMESTAMP AS sales_order_detail__is_current_record,
    CASE
      WHEN sales_order_detail__is_current_record
      THEN sales_order_detail__record_loaded_at
      ELSE sales_order_detail__record_valid_to
    END AS sales_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order_detail|adventure_works|',
      sales_order_detail__sales_order_detail_id,
      '~epoch|valid_from|',
      sales_order_detail__record_valid_from
    ) AS _pit_hook__sales_order_detail,
    CONCAT('sales_order_detail|adventure_works|', sales_order_detail__sales_order_detail_id) AS _hook__sales_order_detail,
    CONCAT('sales_order|adventure_works|', sales_order_detail__sales_order_id) AS _hook__sales_order,
    CONCAT('product|adventure_works|', sales_order_detail__product_id) AS _hook__product,
    CONCAT('special_offer|adventure_works|', sales_order_detail__special_offer_id) AS _hook__special_offer,
    *
  FROM validity
)
SELECT
  _pit_hook__sales_order_detail::BLOB,
  _hook__sales_order_detail::BLOB,
  _hook__product::BLOB,
  _hook__sales_order::BLOB,
  _hook__special_offer::BLOB,
  sales_order_detail__sales_order_detail_id::BIGINT,
  sales_order_detail__product_id::BIGINT,
  sales_order_detail__sales_order_id::BIGINT,
  sales_order_detail__special_offer_id::BIGINT,
  sales_order_detail__carrier_tracking_number::VARCHAR,
  sales_order_detail__line_total::DOUBLE,
  sales_order_detail__modified_date::VARCHAR,
  sales_order_detail__order_qty::BIGINT,
  sales_order_detail__rowguid::VARCHAR,
  sales_order_detail__unit_price::DOUBLE,
  sales_order_detail__unit_price_discount::DOUBLE,
  sales_order_detail__record_loaded_at::TIMESTAMP,
  sales_order_detail__record_updated_at::TIMESTAMP,
  sales_order_detail__record_valid_from::TIMESTAMP,
  sales_order_detail__record_valid_to::TIMESTAMP,
  sales_order_detail__record_version::INT,
  sales_order_detail__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND sales_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts
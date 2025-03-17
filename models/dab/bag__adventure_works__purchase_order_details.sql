MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order_line__purchase
  ),
  tags hook,
  grain (_pit_hook__order_line__purchase, _hook__order_line__purchase),
  description 'Hook viewpoint of purchase_order_details data: Individual products associated with a specific purchase order. See PurchaseOrderHeader.',
  references (_hook__order__purchase, _hook__product),
  column_descriptions (
    purchase_order_detail__purchase_order_id = 'Primary key. Foreign key to PurchaseOrderHeader.PurchaseOrderID.',
    purchase_order_detail__purchase_order_detail_id = 'Primary key. One line number per purchased product.',
    purchase_order_detail__due_date = 'Date the product is expected to be received.',
    purchase_order_detail__order_qty = 'Quantity ordered.',
    purchase_order_detail__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    purchase_order_detail__unit_price = 'Vendor''s selling price of a single product.',
    purchase_order_detail__line_total = 'Per product subtotal. Computed as OrderQty * UnitPrice.',
    purchase_order_detail__received_qty = 'Quantity actually received from the vendor.',
    purchase_order_detail__rejected_qty = 'Quantity rejected during inspection.',
    purchase_order_detail__stocked_qty = 'Quantity accepted into inventory. Computed as ReceivedQty - RejectedQty.',
    purchase_order_detail__record_loaded_at = 'Timestamp when this record was loaded into the system',
    purchase_order_detail__record_updated_at = 'Timestamp when this record was last updated',
    purchase_order_detail__record_version = 'Version number for this record',
    purchase_order_detail__record_valid_from = 'Timestamp from which this record version is valid',
    purchase_order_detail__record_valid_to = 'Timestamp until which this record version is valid',
    purchase_order_detail__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__order_line__purchase = 'Reference hook to purchase order_line',
    _hook__order__purchase = 'Reference hook to purchase order',
    _hook__product = 'Reference hook to product',
    _pit_hook__order_line__purchase = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
  FROM das.raw__adventure_works__purchase_order_details
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
    CONCAT('order_line__purchase__adventure_works|', purchase_order_detail__purchase_order_detail_id) AS _hook__order_line__purchase,
    CONCAT('order__purchase__adventure_works|', purchase_order_detail__purchase_order_id) AS _hook__order__purchase,
    CONCAT('product__adventure_works|', purchase_order_detail__product_id) AS _hook__product,
    CONCAT_WS('~',
      _hook__order_line__purchase,
      'epoch__valid_from|'||purchase_order_detail__record_valid_from
    ) AS _pit_hook__order_line__purchase,
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
  purchase_order_detail__due_date::DATE,
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
  purchase_order_detail__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND purchase_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts
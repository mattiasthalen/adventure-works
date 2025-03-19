MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order_line__purchase),
  description 'Business viewpoint of purchase_order_details data: Individual products associated with a specific purchase order. See PurchaseOrderHeader.',
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
    purchase_order_detail__modified_date = 'Date when this record was last modified',
    purchase_order_detail__record_loaded_at = 'Timestamp when this record was loaded into the system',
    purchase_order_detail__record_updated_at = 'Timestamp when this record was last updated',
    purchase_order_detail__record_version = 'Version number for this record',
    purchase_order_detail__record_valid_from = 'Timestamp from which this record version is valid',
    purchase_order_detail__record_valid_to = 'Timestamp until which this record version is valid',
    purchase_order_detail__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__order_line__purchase,
    purchase_order_detail__purchase_order_id,
    purchase_order_detail__purchase_order_detail_id,
    purchase_order_detail__due_date,
    purchase_order_detail__order_qty,
    purchase_order_detail__product_id,
    purchase_order_detail__unit_price,
    purchase_order_detail__line_total,
    purchase_order_detail__received_qty,
    purchase_order_detail__rejected_qty,
    purchase_order_detail__stocked_qty,
    purchase_order_detail__modified_date,
    purchase_order_detail__record_loaded_at,
    purchase_order_detail__record_updated_at,
    purchase_order_detail__record_version,
    purchase_order_detail__record_valid_from,
    purchase_order_detail__record_valid_to,
    purchase_order_detail__is_current_record
  FROM dab.bag__adventure_works__purchase_order_details
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__order_line__purchase,
    NULL AS purchase_order_detail__purchase_order_id,
    NULL AS purchase_order_detail__purchase_order_detail_id,
    NULL AS purchase_order_detail__due_date,
    NULL AS purchase_order_detail__order_qty,
    NULL AS purchase_order_detail__product_id,
    NULL AS purchase_order_detail__unit_price,
    NULL AS purchase_order_detail__line_total,
    NULL AS purchase_order_detail__received_qty,
    NULL AS purchase_order_detail__rejected_qty,
    NULL AS purchase_order_detail__stocked_qty,
    NULL AS purchase_order_detail__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_detail__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_detail__record_updated_at,
    0 AS purchase_order_detail__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_detail__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS purchase_order_detail__record_valid_to,
    TRUE AS purchase_order_detail__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__order_line__purchase::BLOB,
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
  purchase_order_detail__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.purchase_order_details TO './export/dar/purchase_order_details.parquet' (FORMAT parquet, COMPRESSION zstd)
);
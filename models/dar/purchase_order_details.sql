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

SELECT
  *
  EXCLUDE (_hook__order_line__purchase, _hook__order__purchase, _hook__product)
FROM dab.bag__adventure_works__purchase_order_details
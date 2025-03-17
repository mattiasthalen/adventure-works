MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of purchase_order_details data: Individual products associated with a specific purchase order. See PurchaseOrderHeader.',
  column_descriptions (
    purchase_order_id = 'Primary key. Foreign key to PurchaseOrderHeader.PurchaseOrderID.',
    purchase_order_detail_id = 'Primary key. One line number per purchased product.',
    due_date = 'Date the product is expected to be received.',
    order_qty = 'Quantity ordered.',
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    unit_price = 'Vendor''s selling price of a single product.',
    line_total = 'Per product subtotal. Computed as OrderQty * UnitPrice.',
    received_qty = 'Quantity actually received from the vendor.',
    rejected_qty = 'Quantity rejected during inspection.',
    stocked_qty = 'Quantity accepted into inventory. Computed as ReceivedQty - RejectedQty.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    purchase_order_id::BIGINT,
    purchase_order_detail_id::BIGINT,
    due_date::DATE,
    order_qty::BIGINT,
    product_id::BIGINT,
    unit_price::DOUBLE,
    line_total::DOUBLE,
    received_qty::DOUBLE,
    rejected_qty::DOUBLE,
    stocked_qty::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__purchase_order_details"
)
;
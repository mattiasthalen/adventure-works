MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__purchase),
  description 'Business viewpoint of purchase_order_headers data: General purchase order information. See PurchaseOrderDetail.',
  column_descriptions (
    purchase_order_header__purchase_order_id = 'Primary key.',
    purchase_order_header__revision_number = 'Incremental number to track changes to the purchase order over time.',
    purchase_order_header__status = 'Order current status. 1 = Pending; 2 = Approved; 3 = Rejected; 4 = Complete.',
    purchase_order_header__employee_id = 'Employee who created the purchase order. Foreign key to Employee.BusinessEntityID.',
    purchase_order_header__vendor_id = 'Vendor with whom the purchase order is placed. Foreign key to Vendor.BusinessEntityID.',
    purchase_order_header__ship_method_id = 'Shipping method. Foreign key to ShipMethod.ShipMethodID.',
    purchase_order_header__order_date = 'Purchase order creation date.',
    purchase_order_header__ship_date = 'Estimated shipment date from the vendor.',
    purchase_order_header__sub_total = 'Purchase order subtotal. Computed as SUM(PurchaseOrderDetail.LineTotal) for the appropriate PurchaseOrderID.',
    purchase_order_header__tax_amt = 'Tax amount.',
    purchase_order_header__freight = 'Shipping cost.',
    purchase_order_header__total_due = 'Total due to vendor. Computed as Subtotal + TaxAmt + Freight.',
    purchase_order_header__modified_date = 'Date when this record was last modified',
    purchase_order_header__record_loaded_at = 'Timestamp when this record was loaded into the system',
    purchase_order_header__record_updated_at = 'Timestamp when this record was last updated',
    purchase_order_header__record_version = 'Version number for this record',
    purchase_order_header__record_valid_from = 'Timestamp from which this record version is valid',
    purchase_order_header__record_valid_to = 'Timestamp until which this record version is valid',
    purchase_order_header__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__order__purchase, _hook__person__employee, _hook__vendor, _hook__ship_method)
FROM dab.bag__adventure_works__purchase_order_headers
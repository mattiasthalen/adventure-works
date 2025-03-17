MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of purchase_order_headers data: General purchase order information. See PurchaseOrderDetail.',
  column_descriptions (
    purchase_order_id = 'Primary key.',
    revision_number = 'Incremental number to track changes to the purchase order over time.',
    status = 'Order current status. 1 = Pending; 2 = Approved; 3 = Rejected; 4 = Complete.',
    employee_id = 'Employee who created the purchase order. Foreign key to Employee.BusinessEntityID.',
    vendor_id = 'Vendor with whom the purchase order is placed. Foreign key to Vendor.BusinessEntityID.',
    ship_method_id = 'Shipping method. Foreign key to ShipMethod.ShipMethodID.',
    order_date = 'Purchase order creation date.',
    ship_date = 'Estimated shipment date from the vendor.',
    sub_total = 'Purchase order subtotal. Computed as SUM(PurchaseOrderDetail.LineTotal) for the appropriate PurchaseOrderID.',
    tax_amt = 'Tax amount.',
    freight = 'Shipping cost.',
    total_due = 'Total due to vendor. Computed as Subtotal + TaxAmt + Freight.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    purchase_order_id::BIGINT,
    revision_number::BIGINT,
    status::BIGINT,
    employee_id::BIGINT,
    vendor_id::BIGINT,
    ship_method_id::BIGINT,
    order_date::DATE,
    ship_date::DATE,
    sub_total::DOUBLE,
    tax_amt::DOUBLE,
    freight::DOUBLE,
    total_due::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__purchase_order_headers"
)
;
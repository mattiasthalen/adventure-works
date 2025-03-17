MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_order_details data: Individual products associated with a specific sales order. See SalesOrderHeader.',
  column_descriptions (
    sales_order_id = 'Primary key. Foreign key to SalesOrderHeader.SalesOrderID.',
    sales_order_detail_id = 'Primary key. One incremental unique number per product sold.',
    carrier_tracking_number = 'Shipment tracking number supplied by the shipper.',
    order_qty = 'Quantity ordered per product.',
    product_id = 'Product sold to customer. Foreign key to Product.ProductID.',
    special_offer_id = 'Promotional code. Foreign key to SpecialOffer.SpecialOfferID.',
    unit_price = 'Selling price of a single product.',
    unit_price_discount = 'Discount amount.',
    line_total = 'Per product subtotal. Computed as UnitPrice * (1 - UnitPriceDiscount) * OrderQty.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    sales_order_id::BIGINT,
    sales_order_detail_id::BIGINT,
    carrier_tracking_number::TEXT,
    order_qty::BIGINT,
    product_id::BIGINT,
    special_offer_id::BIGINT,
    unit_price::DOUBLE,
    unit_price_discount::DOUBLE,
    line_total::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_order_details"
)
;
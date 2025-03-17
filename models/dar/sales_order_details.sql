MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order_line__sales),
  description 'Business viewpoint of sales_order_details data: Individual products associated with a specific sales order. See SalesOrderHeader.',
  column_descriptions (
    sales_order_detail__sales_order_id = 'Primary key. Foreign key to SalesOrderHeader.SalesOrderID.',
    sales_order_detail__sales_order_detail_id = 'Primary key. One incremental unique number per product sold.',
    sales_order_detail__carrier_tracking_number = 'Shipment tracking number supplied by the shipper.',
    sales_order_detail__order_qty = 'Quantity ordered per product.',
    sales_order_detail__product_id = 'Product sold to customer. Foreign key to Product.ProductID.',
    sales_order_detail__special_offer_id = 'Promotional code. Foreign key to SpecialOffer.SpecialOfferID.',
    sales_order_detail__unit_price = 'Selling price of a single product.',
    sales_order_detail__unit_price_discount = 'Discount amount.',
    sales_order_detail__line_total = 'Per product subtotal. Computed as UnitPrice * (1 - UnitPriceDiscount) * OrderQty.',
    sales_order_detail__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_order_detail__modified_date = 'Date when this record was last modified',
    sales_order_detail__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_order_detail__record_updated_at = 'Timestamp when this record was last updated',
    sales_order_detail__record_version = 'Version number for this record',
    sales_order_detail__record_valid_from = 'Timestamp from which this record version is valid',
    sales_order_detail__record_valid_to = 'Timestamp until which this record version is valid',
    sales_order_detail__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__order_line__sales, _hook__order__sales, _hook__product, _hook__reference__special_offer)
FROM dab.bag__adventure_works__sales_order_details
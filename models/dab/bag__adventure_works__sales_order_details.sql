MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order_line__sales
  ),
  tags hook,
  grain (_pit_hook__order_line__sales, _hook__order_line__sales),
  description 'Hook viewpoint of sales_order_details data: Individual products associated with a specific sales order. See SalesOrderHeader.',
  references (_hook__order__sales, _hook__product, _hook__reference__special_offer),
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
    sales_order_detail__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_order_detail__record_updated_at = 'Timestamp when this record was last updated',
    sales_order_detail__record_version = 'Version number for this record',
    sales_order_detail__record_valid_from = 'Timestamp from which this record version is valid',
    sales_order_detail__record_valid_to = 'Timestamp until which this record version is valid',
    sales_order_detail__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__order_line__sales = 'Reference hook to sales order_line',
    _hook__order__sales = 'Reference hook to sales order',
    _hook__product = 'Reference hook to product',
    _hook__reference__special_offer = 'Reference hook to special_offer reference',
    _pit_hook__order_line__sales = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    sales_order_id AS sales_order_detail__sales_order_id,
    sales_order_detail_id AS sales_order_detail__sales_order_detail_id,
    carrier_tracking_number AS sales_order_detail__carrier_tracking_number,
    order_qty AS sales_order_detail__order_qty,
    product_id AS sales_order_detail__product_id,
    special_offer_id AS sales_order_detail__special_offer_id,
    unit_price AS sales_order_detail__unit_price,
    unit_price_discount AS sales_order_detail__unit_price_discount,
    line_total AS sales_order_detail__line_total,
    rowguid AS sales_order_detail__rowguid,
    modified_date AS sales_order_detail__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order_detail__record_loaded_at
  FROM das.raw__adventure_works__sales_order_details
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_detail__sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at) AS sales_order_detail__record_version,
    CASE
      WHEN sales_order_detail__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_order_detail__record_loaded_at
    END AS sales_order_detail__record_valid_from,
    COALESCE(
      LEAD(sales_order_detail__record_loaded_at) OVER (PARTITION BY sales_order_detail__sales_order_detail_id ORDER BY sales_order_detail__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_order_detail__record_valid_to,
    sales_order_detail__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_order_detail__is_current_record,
    CASE
      WHEN sales_order_detail__is_current_record
      THEN sales_order_detail__record_loaded_at
      ELSE sales_order_detail__record_valid_to
    END AS sales_order_detail__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('order_line__sales__adventure_works|', sales_order_detail__sales_order_detail_id) AS _hook__order_line__sales,
    CONCAT('order__sales__adventure_works|', sales_order_detail__sales_order_id) AS _hook__order__sales,
    CONCAT('product__adventure_works|', sales_order_detail__product_id) AS _hook__product,
    CONCAT('reference__special_offer__adventure_works|', sales_order_detail__special_offer_id) AS _hook__reference__special_offer,
    CONCAT_WS('~',
      _hook__order_line__sales,
      'epoch__valid_from|'||sales_order_detail__record_valid_from
    ) AS _pit_hook__order_line__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__order_line__sales::BLOB,
  _hook__order_line__sales::BLOB,
  _hook__order__sales::BLOB,
  _hook__product::BLOB,
  _hook__reference__special_offer::BLOB,
  sales_order_detail__sales_order_id::BIGINT,
  sales_order_detail__sales_order_detail_id::BIGINT,
  sales_order_detail__carrier_tracking_number::TEXT,
  sales_order_detail__order_qty::BIGINT,
  sales_order_detail__product_id::BIGINT,
  sales_order_detail__special_offer_id::BIGINT,
  sales_order_detail__unit_price::DOUBLE,
  sales_order_detail__unit_price_discount::DOUBLE,
  sales_order_detail__line_total::DOUBLE,
  sales_order_detail__rowguid::TEXT,
  sales_order_detail__modified_date::DATE,
  sales_order_detail__record_loaded_at::TIMESTAMP,
  sales_order_detail__record_updated_at::TIMESTAMP,
  sales_order_detail__record_version::TEXT,
  sales_order_detail__record_valid_from::TIMESTAMP,
  sales_order_detail__record_valid_to::TIMESTAMP,
  sales_order_detail__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts
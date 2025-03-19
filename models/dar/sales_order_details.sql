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

WITH cte__source AS (
  SELECT
    _pit_hook__order_line__sales,
    sales_order_detail__sales_order_id,
    sales_order_detail__sales_order_detail_id,
    sales_order_detail__carrier_tracking_number,
    sales_order_detail__order_qty,
    sales_order_detail__product_id,
    sales_order_detail__special_offer_id,
    sales_order_detail__unit_price,
    sales_order_detail__unit_price_discount,
    sales_order_detail__line_total,
    sales_order_detail__rowguid,
    sales_order_detail__modified_date,
    sales_order_detail__record_loaded_at,
    sales_order_detail__record_updated_at,
    sales_order_detail__record_version,
    sales_order_detail__record_valid_from,
    sales_order_detail__record_valid_to,
    sales_order_detail__is_current_record
  FROM dab.bag__adventure_works__sales_order_details
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__order_line__sales,
    NULL AS sales_order_detail__sales_order_id,
    NULL AS sales_order_detail__sales_order_detail_id,
    'N/A' AS sales_order_detail__carrier_tracking_number,
    NULL AS sales_order_detail__order_qty,
    NULL AS sales_order_detail__product_id,
    NULL AS sales_order_detail__special_offer_id,
    NULL AS sales_order_detail__unit_price,
    NULL AS sales_order_detail__unit_price_discount,
    NULL AS sales_order_detail__line_total,
    'N/A' AS sales_order_detail__rowguid,
    NULL AS sales_order_detail__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_detail__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_detail__record_updated_at,
    0 AS sales_order_detail__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_detail__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_order_detail__record_valid_to,
    TRUE AS sales_order_detail__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__order_line__sales::BLOB,
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
  sales_order_detail__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.sales_order_details TO './export/dar/sales_order_details.parquet' (FORMAT parquet, COMPRESSION zstd)
);
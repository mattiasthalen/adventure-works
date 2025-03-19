MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_vendor),
  description 'Business viewpoint of product_vendors data: Cross-reference table mapping vendors with the products they supply.',
  column_descriptions (
    product_vendor__product_id = 'Primary key. Foreign key to Product.ProductID.',
    product_vendor__business_entity_id = 'Primary key. Foreign key to Vendor.BusinessEntityID.',
    product_vendor__average_lead_time = 'The average span of time (in days) between placing an order with the vendor and receiving the purchased product.',
    product_vendor__standard_price = 'The vendor''s usual selling price.',
    product_vendor__last_receipt_cost = 'The selling price when last purchased.',
    product_vendor__last_receipt_date = 'Date the product was last received by the vendor.',
    product_vendor__min_order_qty = 'The minimum quantity that should be ordered.',
    product_vendor__max_order_qty = 'The maximum quantity that should be ordered.',
    product_vendor__unit_measure_code = 'The product''s unit of measure.',
    product_vendor__on_order_qty = 'The quantity currently on order.',
    product_vendor__modified_date = 'Date when this record was last modified',
    product_vendor__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_vendor__record_updated_at = 'Timestamp when this record was last updated',
    product_vendor__record_version = 'Version number for this record',
    product_vendor__record_valid_from = 'Timestamp from which this record version is valid',
    product_vendor__record_valid_to = 'Timestamp until which this record version is valid',
    product_vendor__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_vendor,
    product_vendor__product_id,
    product_vendor__business_entity_id,
    product_vendor__average_lead_time,
    product_vendor__standard_price,
    product_vendor__last_receipt_cost,
    product_vendor__last_receipt_date,
    product_vendor__min_order_qty,
    product_vendor__max_order_qty,
    product_vendor__unit_measure_code,
    product_vendor__on_order_qty,
    product_vendor__modified_date,
    product_vendor__record_loaded_at,
    product_vendor__record_updated_at,
    product_vendor__record_version,
    product_vendor__record_valid_from,
    product_vendor__record_valid_to,
    product_vendor__is_current_record
  FROM dab.bag__adventure_works__product_vendors
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_vendor,
    NULL AS product_vendor__product_id,
    NULL AS product_vendor__business_entity_id,
    NULL AS product_vendor__average_lead_time,
    NULL AS product_vendor__standard_price,
    NULL AS product_vendor__last_receipt_cost,
    NULL AS product_vendor__last_receipt_date,
    NULL AS product_vendor__min_order_qty,
    NULL AS product_vendor__max_order_qty,
    'N/A' AS product_vendor__unit_measure_code,
    NULL AS product_vendor__on_order_qty,
    NULL AS product_vendor__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_vendor__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_vendor__record_updated_at,
    0 AS product_vendor__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_vendor__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_vendor__record_valid_to,
    TRUE AS product_vendor__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_vendor::BLOB,
  product_vendor__product_id::BIGINT,
  product_vendor__business_entity_id::BIGINT,
  product_vendor__average_lead_time::BIGINT,
  product_vendor__standard_price::DOUBLE,
  product_vendor__last_receipt_cost::DOUBLE,
  product_vendor__last_receipt_date::DATE,
  product_vendor__min_order_qty::BIGINT,
  product_vendor__max_order_qty::BIGINT,
  product_vendor__unit_measure_code::TEXT,
  product_vendor__on_order_qty::BIGINT,
  product_vendor__modified_date::DATE,
  product_vendor__record_loaded_at::TIMESTAMP,
  product_vendor__record_updated_at::TIMESTAMP,
  product_vendor__record_version::TEXT,
  product_vendor__record_valid_from::TIMESTAMP,
  product_vendor__record_valid_to::TIMESTAMP,
  product_vendor__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.product_vendors TO './export/dar/product_vendors.parquet' (FORMAT parquet, COMPRESSION zstd)
);
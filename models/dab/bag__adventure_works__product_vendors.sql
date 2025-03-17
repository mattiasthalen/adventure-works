MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product_vendor
  ),
  tags hook,
  grain (_pit_hook__product_vendor, _hook__product_vendor),
  description 'Hook viewpoint of product_vendors data: Cross-reference table mapping vendors with the products they supply.',
  references (_hook__vendor, _hook__product, _hook__reference__unit_measure),
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
    product_vendor__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_vendor__record_updated_at = 'Timestamp when this record was last updated',
    product_vendor__record_version = 'Version number for this record',
    product_vendor__record_valid_from = 'Timestamp from which this record version is valid',
    product_vendor__record_valid_to = 'Timestamp until which this record version is valid',
    product_vendor__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__product_vendor = 'Reference hook to product_vendor',
    _hook__vendor = 'Reference hook to vendor',
    _hook__product = 'Reference hook to product',
    _hook__reference__unit_measure = 'Reference hook to unit_measure reference',
    _pit_hook__product_vendor = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    product_id AS product_vendor__product_id,
    business_entity_id AS product_vendor__business_entity_id,
    average_lead_time AS product_vendor__average_lead_time,
    standard_price AS product_vendor__standard_price,
    last_receipt_cost AS product_vendor__last_receipt_cost,
    last_receipt_date AS product_vendor__last_receipt_date,
    min_order_qty AS product_vendor__min_order_qty,
    max_order_qty AS product_vendor__max_order_qty,
    unit_measure_code AS product_vendor__unit_measure_code,
    on_order_qty AS product_vendor__on_order_qty,
    modified_date AS product_vendor__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_vendor__record_loaded_at
  FROM das.raw__adventure_works__product_vendors
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_vendor__business_entity_id, product_vendor__product_id ORDER BY product_vendor__record_loaded_at) AS product_vendor__record_version,
    CASE
      WHEN product_vendor__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_vendor__record_loaded_at
    END AS product_vendor__record_valid_from,
    COALESCE(
      LEAD(product_vendor__record_loaded_at) OVER (PARTITION BY product_vendor__business_entity_id, product_vendor__product_id ORDER BY product_vendor__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_vendor__record_valid_to,
    product_vendor__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_vendor__is_current_record,
    CASE
      WHEN product_vendor__is_current_record
      THEN product_vendor__record_loaded_at
      ELSE product_vendor__record_valid_to
    END AS product_vendor__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('vendor__adventure_works|', product_vendor__business_entity_id) AS _hook__vendor,
    CONCAT('product__adventure_works|', product_vendor__product_id) AS _hook__product,
    CONCAT('reference__unit_measure__adventure_works|', product_vendor__unit_measure_code) AS _hook__reference__unit_measure,
    CONCAT_WS('~', _hook__vendor, _hook__product) AS _hook__product_vendor,
    CONCAT_WS('~',
      _hook__product_vendor,
      'epoch__valid_from|'||product_vendor__record_valid_from
    ) AS _pit_hook__product_vendor,
    *
  FROM validity
)
SELECT
  _pit_hook__product_vendor::BLOB,
  _hook__product_vendor::BLOB,
  _hook__vendor::BLOB,
  _hook__product::BLOB,
  _hook__reference__unit_measure::BLOB,
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
  product_vendor__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_vendor__record_updated_at BETWEEN @start_ts AND @end_ts
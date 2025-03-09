MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__vendor,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__vendor, _hook__vendor),
  references (_hook__product, _hook__reference__unit_measure)
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
    ROW_NUMBER() OVER (PARTITION BY product_vendor__business_entity_id ORDER BY product_vendor__record_loaded_at) AS product_vendor__record_version,
    CASE
      WHEN product_vendor__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_vendor__record_loaded_at
    END AS product_vendor__record_valid_from,
    COALESCE(
      LEAD(product_vendor__record_loaded_at) OVER (PARTITION BY product_vendor__business_entity_id ORDER BY product_vendor__record_loaded_at),
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
    CONCAT(
      'vendor__adventure_works|',
      product_vendor__business_entity_id,
      '~epoch__valid_from|',
      product_vendor__record_valid_from
    )::BLOB AS _pit_hook__vendor,
    CONCAT('vendor__adventure_works|', product_vendor__business_entity_id) AS _hook__vendor,
    CONCAT('product__adventure_works|', product_vendor__product_id) AS _hook__product,
    CONCAT('reference__unit_measure__adventure_works|', product_vendor__unit_measure_code) AS _hook__reference__unit_measure,
    *
  FROM validity
)
SELECT
  _pit_hook__vendor::BLOB,
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
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_vendor__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS product_vendor__business_entity_id,
    product_id AS product_vendor__product_id,
    average_lead_time AS product_vendor__average_lead_time,
    last_receipt_cost AS product_vendor__last_receipt_cost,
    last_receipt_date AS product_vendor__last_receipt_date,
    max_order_qty AS product_vendor__max_order_qty,
    min_order_qty AS product_vendor__min_order_qty,
    modified_date AS product_vendor__modified_date,
    on_order_qty AS product_vendor__on_order_qty,
    standard_price AS product_vendor__standard_price,
    unit_measure_code AS product_vendor__unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_vendor__record_loaded_at
  FROM bronze.raw__adventure_works__product_vendors
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_vendor__business_entity_id ORDER BY product_vendor__record_loaded_at) AS product_vendor__record_version,
    CASE
      WHEN product_vendor__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_vendor__record_loaded_at
    END AS product_vendor__record_valid_from,
    COALESCE(
      LEAD(product_vendor__record_loaded_at) OVER (PARTITION BY product_vendor__business_entity_id ORDER BY product_vendor__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_vendor__record_valid_to,
    product_vendor__record_valid_to = @max_ts::TIMESTAMP AS product_vendor__is_current_record,
    CASE
      WHEN product_vendor__is_current_record
      THEN product_vendor__record_loaded_at
      ELSE product_vendor__record_valid_to
    END AS product_vendor__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      product_vendor__business_entity_id,
      '~epoch|valid_from|',
      product_vendor__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', product_vendor__business_entity_id) AS _hook__business_entity,
    CONCAT('product|adventure_works|', product_vendor__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__product::BLOB,
  product_vendor__business_entity_id::BIGINT,
  product_vendor__product_id::BIGINT,
  product_vendor__average_lead_time::BIGINT,
  product_vendor__last_receipt_cost::DOUBLE,
  product_vendor__last_receipt_date::VARCHAR,
  product_vendor__max_order_qty::BIGINT,
  product_vendor__min_order_qty::BIGINT,
  product_vendor__modified_date::VARCHAR,
  product_vendor__on_order_qty::BIGINT,
  product_vendor__standard_price::DOUBLE,
  product_vendor__unit_measure_code::VARCHAR,
  product_vendor__record_loaded_at::TIMESTAMP,
  product_vendor__record_updated_at::TIMESTAMP,
  product_vendor__record_valid_from::TIMESTAMP,
  product_vendor__record_valid_to::TIMESTAMP,
  product_vendor__record_version::INT,
  product_vendor__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_vendor__record_updated_at BETWEEN @start_ts AND @end_ts
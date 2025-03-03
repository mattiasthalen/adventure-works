MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity__business_entity_id,
    product_id AS business_entity__product_id,
    average_lead_time AS business_entity__average_lead_time,
    last_receipt_cost AS business_entity__last_receipt_cost,
    last_receipt_date AS business_entity__last_receipt_date,
    max_order_qty AS business_entity__max_order_qty,
    min_order_qty AS business_entity__min_order_qty,
    modified_date AS business_entity__modified_date,
    on_order_qty AS business_entity__on_order_qty,
    standard_price AS business_entity__standard_price,
    unit_measure_code AS business_entity__unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM bronze.raw__adventure_works__product_vendors
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at) AS business_entity__record_version,
    CASE
      WHEN business_entity__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE business_entity__record_loaded_at
    END AS business_entity__record_valid_from,
    COALESCE(
      LEAD(business_entity__record_loaded_at) OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS business_entity__record_valid_to,
    business_entity__record_valid_to = @max_ts::TIMESTAMP AS business_entity__is_current_record,
    CASE
      WHEN business_entity__is_current_record
      THEN business_entity__record_loaded_at
      ELSE business_entity__record_valid_to
    END AS business_entity__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      business_entity__business_entity_id,
      '~epoch|valid_from|',
      business_entity__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity__business_entity_id) AS _hook__business_entity,
    CONCAT('product|adventure_works|', business_entity__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__product::BLOB,
  business_entity__business_entity_id::VARCHAR,
  business_entity__product_id::VARCHAR,
  business_entity__average_lead_time::VARCHAR,
  business_entity__last_receipt_cost::VARCHAR,
  business_entity__last_receipt_date::VARCHAR,
  business_entity__max_order_qty::VARCHAR,
  business_entity__min_order_qty::VARCHAR,
  business_entity__modified_date::VARCHAR,
  business_entity__on_order_qty::VARCHAR,
  business_entity__standard_price::VARCHAR,
  business_entity__unit_measure_code::VARCHAR,
  business_entity__record_loaded_at::TIMESTAMP,
  business_entity__record_version::INT,
  business_entity__record_valid_from::TIMESTAMP,
  business_entity__record_valid_to::TIMESTAMP,
  business_entity__is_current_record::BOOLEAN,
  business_entity__record_updated_at::TIMESTAMP
FROM hooks
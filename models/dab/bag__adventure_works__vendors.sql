MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__vendor,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__vendor, _hook__vendor)
);

WITH staging AS (
  SELECT
    business_entity_id AS vendor__business_entity_id,
    account_number AS vendor__account_number,
    name AS vendor__name,
    credit_rating AS vendor__credit_rating,
    preferred_vendor_status AS vendor__preferred_vendor_status,
    active_flag AS vendor__active_flag,
    purchasing_web_service_url AS vendor__purchasing_web_service_url,
    modified_date AS vendor__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS vendor__record_loaded_at
  FROM das.raw__adventure_works__vendors
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY vendor__business_entity_id ORDER BY vendor__record_loaded_at) AS vendor__record_version,
    CASE
      WHEN vendor__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE vendor__record_loaded_at
    END AS vendor__record_valid_from,
    COALESCE(
      LEAD(vendor__record_loaded_at) OVER (PARTITION BY vendor__business_entity_id ORDER BY vendor__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS vendor__record_valid_to,
    vendor__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS vendor__is_current_record,
    CASE
      WHEN vendor__is_current_record
      THEN vendor__record_loaded_at
      ELSE vendor__record_valid_to
    END AS vendor__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'vendor__adventure_works|',
      vendor__business_entity_id,
      '~epoch|valid_from|',
      vendor__record_valid_from
    )::BLOB AS _pit_hook__vendor,
    CONCAT('vendor__adventure_works|', vendor__business_entity_id) AS _hook__vendor,
    *
  FROM validity
)
SELECT
  _pit_hook__vendor::BLOB,
  _hook__vendor::BLOB,
  vendor__business_entity_id::BIGINT,
  vendor__account_number::TEXT,
  vendor__name::TEXT,
  vendor__credit_rating::BIGINT,
  vendor__preferred_vendor_status::BOOLEAN,
  vendor__active_flag::BOOLEAN,
  vendor__purchasing_web_service_url::TEXT,
  vendor__modified_date::DATE,
  vendor__record_loaded_at::TIMESTAMP,
  vendor__record_updated_at::TIMESTAMP,
  vendor__record_version::TEXT,
  vendor__record_valid_from::TIMESTAMP,
  vendor__record_valid_to::TIMESTAMP,
  vendor__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND vendor__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column vendor__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS vendor__business_entity_id,
    account_number AS vendor__account_number,
    active_flag AS vendor__active_flag,
    credit_rating AS vendor__credit_rating,
    modified_date AS vendor__modified_date,
    name AS vendor__name,
    preferred_vendor_status AS vendor__preferred_vendor_status,
    purchasing_web_service_url AS vendor__purchasing_web_service_url,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS vendor__record_loaded_at
  FROM bronze.raw__adventure_works__vendors
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY vendor__business_entity_id ORDER BY vendor__record_loaded_at) AS vendor__record_version,
    CASE
      WHEN vendor__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE vendor__record_loaded_at
    END AS vendor__record_valid_from,
    COALESCE(
      LEAD(vendor__record_loaded_at) OVER (PARTITION BY vendor__business_entity_id ORDER BY vendor__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS vendor__record_valid_to,
    vendor__record_valid_to = @max_ts::TIMESTAMP AS vendor__is_current_record,
    CASE
      WHEN vendor__is_current_record
      THEN vendor__record_loaded_at
      ELSE vendor__record_valid_to
    END AS vendor__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      vendor__business_entity_id,
      '~epoch|valid_from|',
      vendor__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', vendor__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  vendor__business_entity_id::BIGINT,
  vendor__account_number::VARCHAR,
  vendor__active_flag::BOOLEAN,
  vendor__credit_rating::BIGINT,
  vendor__modified_date::VARCHAR,
  vendor__name::VARCHAR,
  vendor__preferred_vendor_status::BOOLEAN,
  vendor__purchasing_web_service_url::VARCHAR,
  vendor__record_loaded_at::TIMESTAMP,
  vendor__record_updated_at::TIMESTAMP,
  vendor__record_valid_from::TIMESTAMP,
  vendor__record_valid_to::TIMESTAMP,
  vendor__record_version::INT,
  vendor__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND vendor__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column email_address__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS email_address__business_entity_id,
    email_address_id AS email_address__email_address_id,
    email AS email_address__email,
    modified_date AS email_address__modified_date,
    rowguid AS email_address__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS email_address__record_loaded_at
  FROM bronze.raw__adventure_works__email_addresses
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY email_address__business_entity_id ORDER BY email_address__record_loaded_at) AS email_address__record_version,
    CASE
      WHEN email_address__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE email_address__record_loaded_at
    END AS email_address__record_valid_from,
    COALESCE(
      LEAD(email_address__record_loaded_at) OVER (PARTITION BY email_address__business_entity_id ORDER BY email_address__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS email_address__record_valid_to,
    email_address__record_valid_to = @max_ts::TIMESTAMP AS email_address__is_current_record,
    CASE
      WHEN email_address__is_current_record
      THEN email_address__record_loaded_at
      ELSE email_address__record_valid_to
    END AS email_address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      email_address__business_entity_id,
      '~epoch|valid_from|',
      email_address__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', email_address__business_entity_id) AS _hook__business_entity,
    CONCAT('email_address|adventure_works|', email_address__email_address_id) AS _hook__email_address,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__email_address::BLOB,
  email_address__business_entity_id::BIGINT,
  email_address__email_address_id::BIGINT,
  email_address__email::VARCHAR,
  email_address__modified_date::VARCHAR,
  email_address__rowguid::VARCHAR,
  email_address__record_loaded_at::TIMESTAMP,
  email_address__record_updated_at::TIMESTAMP,
  email_address__record_valid_from::TIMESTAMP,
  email_address__record_valid_to::TIMESTAMP,
  email_address__record_version::INT,
  email_address__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND email_address__record_updated_at BETWEEN @start_ts AND @end_ts
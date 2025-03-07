MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__individual,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__individual, _hook__person__individual),
  references (_hook__reference__phone_number_type)
);

WITH staging AS (
  SELECT
    business_entity_id AS person_phone__business_entity_id,
    phone_number AS person_phone__phone_number,
    phone_number_type_id AS person_phone__phone_number_type_id,
    modified_date AS person_phone__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person_phone__record_loaded_at
  FROM das.raw__adventure_works__person_phones
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person_phone__business_entity_id ORDER BY person_phone__record_loaded_at) AS person_phone__record_version,
    CASE
      WHEN person_phone__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE person_phone__record_loaded_at
    END AS person_phone__record_valid_from,
    COALESCE(
      LEAD(person_phone__record_loaded_at) OVER (PARTITION BY person_phone__business_entity_id ORDER BY person_phone__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS person_phone__record_valid_to,
    person_phone__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS person_phone__is_current_record,
    CASE
      WHEN person_phone__is_current_record
      THEN person_phone__record_loaded_at
      ELSE person_phone__record_valid_to
    END AS person_phone__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'person__individual__adventure_works|',
      person_phone__business_entity_id,
      '~epoch|valid_from|',
      person_phone__record_valid_from
    )::BLOB AS _pit_hook__person__individual,
    CONCAT('person__individual__adventure_works|', person_phone__business_entity_id) AS _hook__person__individual,
    CONCAT('reference__phone_number_type__adventure_works|', person_phone__phone_number_type_id) AS _hook__reference__phone_number_type,
    *
  FROM validity
)
SELECT
  _pit_hook__person__individual::BLOB,
  _hook__person__individual::BLOB,
  _hook__reference__phone_number_type::BLOB,
  person_phone__business_entity_id::BIGINT,
  person_phone__phone_number::TEXT,
  person_phone__phone_number_type_id::BIGINT,
  person_phone__modified_date::DATE,
  person_phone__record_loaded_at::TIMESTAMP,
  person_phone__record_updated_at::TIMESTAMP,
  person_phone__record_version::TEXT,
  person_phone__record_valid_from::TIMESTAMP,
  person_phone__record_valid_to::TIMESTAMP,
  person_phone__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND person_phone__record_updated_at BETWEEN @start_ts AND @end_ts
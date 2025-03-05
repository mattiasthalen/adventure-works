MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column business_entity_contact__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__business_entity, _hook__business_entity),
  references (_hook__person__contact, _hook__reference__contact_type)
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity_contact__business_entity_id,
    person_id AS business_entity_contact__person_id,
    contact_type_id AS business_entity_contact__contact_type_id,
    rowguid AS business_entity_contact__rowguid,
    modified_date AS business_entity_contact__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity_contact__record_loaded_at
  FROM bronze.raw__adventure_works__business_entity_contacts
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at) AS business_entity_contact__record_version,
    CASE
      WHEN business_entity_contact__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE business_entity_contact__record_loaded_at
    END AS business_entity_contact__record_valid_from,
    COALESCE(
      LEAD(business_entity_contact__record_loaded_at) OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS business_entity_contact__record_valid_to,
    business_entity_contact__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS business_entity_contact__is_current_record,
    CASE
      WHEN business_entity_contact__is_current_record
      THEN business_entity_contact__record_loaded_at
      ELSE business_entity_contact__record_valid_to
    END AS business_entity_contact__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity__adventure_works|',
      business_entity_contact__business_entity_id,
      '~epoch__valid_from|',
      business_entity_contact__record_valid_from
    )::BLOB AS _pit_hook__business_entity,
    CONCAT('business_entity__adventure_works|', business_entity_contact__business_entity_id) AS _hook__business_entity,
    CONCAT('person__individual__adventure_works|', business_entity_contact__person_id) AS _hook__person__contact,
    CONCAT('reference__contact_type__adventure_works|', business_entity_contact__contact_type_id) AS _hook__reference__contact_type,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__person__contact::BLOB,
  _hook__reference__contact_type::BLOB,
  business_entity_contact__business_entity_id::BIGINT,
  business_entity_contact__person_id::BIGINT,
  business_entity_contact__contact_type_id::BIGINT,
  business_entity_contact__rowguid::TEXT,
  business_entity_contact__modified_date::DATE,
  business_entity_contact__record_loaded_at::TIMESTAMP,
  business_entity_contact__record_updated_at::TIMESTAMP,
  business_entity_contact__record_version::TEXT,
  business_entity_contact__record_valid_from::TIMESTAMP,
  business_entity_contact__record_valid_to::TIMESTAMP,
  business_entity_contact__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND business_entity_contact__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column business_entity_contact__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity_contact__business_entity_id,
    contact_type_id AS business_entity_contact__contact_type_id,
    person_id AS business_entity_contact__person_id,
    modified_date AS business_entity_contact__modified_date,
    rowguid AS business_entity_contact__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity_contact__record_loaded_at
  FROM bronze.raw__adventure_works__business_entity_contacts
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at) AS business_entity_contact__record_version,
    CASE
      WHEN business_entity_contact__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE business_entity_contact__record_loaded_at
    END AS business_entity_contact__record_valid_from,
    COALESCE(
      LEAD(business_entity_contact__record_loaded_at) OVER (PARTITION BY business_entity_contact__business_entity_id ORDER BY business_entity_contact__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS business_entity_contact__record_valid_to,
    business_entity_contact__record_valid_to = @max_ts::TIMESTAMP AS business_entity_contact__is_current_record,
    CASE
      WHEN business_entity_contact__is_current_record
      THEN business_entity_contact__record_loaded_at
      ELSE business_entity_contact__record_valid_to
    END AS business_entity_contact__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      business_entity_contact__business_entity_id,
      '~epoch|valid_from|',
      business_entity_contact__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity_contact__business_entity_id) AS _hook__business_entity,
    CONCAT('person|adventure_works|', business_entity_contact__person_id) AS _hook__person,
    CONCAT('contact_type|adventure_works|', business_entity_contact__contact_type_id) AS _hook__contact_type,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__contact_type::BLOB,
  _hook__person::BLOB,
  business_entity_contact__business_entity_id::BIGINT,
  business_entity_contact__contact_type_id::BIGINT,
  business_entity_contact__person_id::BIGINT,
  business_entity_contact__modified_date::VARCHAR,
  business_entity_contact__rowguid::VARCHAR,
  business_entity_contact__record_loaded_at::TIMESTAMP,
  business_entity_contact__record_updated_at::TIMESTAMP,
  business_entity_contact__record_valid_from::TIMESTAMP,
  business_entity_contact__record_valid_to::TIMESTAMP,
  business_entity_contact__record_version::INT,
  business_entity_contact__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND business_entity_contact__record_updated_at BETWEEN @start_ts AND @end_ts
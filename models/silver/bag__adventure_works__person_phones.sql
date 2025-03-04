MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column person_phon__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS person_phon__business_entity_id,
    phone_number_type_id AS person_phon__phone_number_type_id,
    modified_date AS person_phon__modified_date,
    phone_number AS person_phon__phone_number,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person_phon__record_loaded_at
  FROM bronze.raw__adventure_works__person_phones
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person_phon__business_entity_id ORDER BY person_phon__record_loaded_at) AS person_phon__record_version,
    CASE
      WHEN person_phon__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE person_phon__record_loaded_at
    END AS person_phon__record_valid_from,
    COALESCE(
      LEAD(person_phon__record_loaded_at) OVER (PARTITION BY person_phon__business_entity_id ORDER BY person_phon__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS person_phon__record_valid_to,
    person_phon__record_valid_to = @max_ts::TIMESTAMP AS person_phon__is_current_record,
    CASE
      WHEN person_phon__is_current_record
      THEN person_phon__record_loaded_at
      ELSE person_phon__record_valid_to
    END AS person_phon__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      person_phon__business_entity_id,
      '~epoch|valid_from|',
      person_phon__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', person_phon__business_entity_id) AS _hook__business_entity,
    CONCAT('phone_number_type|adventure_works|', person_phon__phone_number_type_id) AS _hook__phone_number_type,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__phone_number_type::BLOB,
  person_phon__business_entity_id::BIGINT,
  person_phon__phone_number_type_id::BIGINT,
  person_phon__modified_date::VARCHAR,
  person_phon__phone_number::VARCHAR,
  person_phon__record_loaded_at::TIMESTAMP,
  person_phon__record_updated_at::TIMESTAMP,
  person_phon__record_valid_from::TIMESTAMP,
  person_phon__record_valid_to::TIMESTAMP,
  person_phon__record_version::INT,
  person_phon__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND person_phon__record_updated_at BETWEEN @start_ts AND @end_ts
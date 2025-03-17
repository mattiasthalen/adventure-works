MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__individual
  ),
  tags hook,
  grain (_pit_hook__person__individual, _hook__person__individual),
  description 'Hook viewpoint of person_phones data: Telephone number and type of a person.',
  references (_hook__reference__phone_number_type),
  column_descriptions (
    person_phone__business_entity_id = 'Business entity identification number. Foreign key to Person.BusinessEntityID.',
    person_phone__phone_number = 'Telephone number identification number.',
    person_phone__phone_number_type_id = 'Kind of phone number. Foreign key to PhoneNumberType.PhoneNumberTypeID.',
    person_phone__record_loaded_at = 'Timestamp when this record was loaded into the system',
    person_phone__record_updated_at = 'Timestamp when this record was last updated',
    person_phone__record_version = 'Version number for this record',
    person_phone__record_valid_from = 'Timestamp from which this record version is valid',
    person_phone__record_valid_to = 'Timestamp until which this record version is valid',
    person_phone__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__person__individual = 'Reference hook to individual person',
    _hook__reference__phone_number_type = 'Reference hook to phone_number_type reference',
    _pit_hook__person__individual = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
    CONCAT('person__individual__adventure_works|', person_phone__business_entity_id) AS _hook__person__individual,
    CONCAT('reference__phone_number_type__adventure_works|', person_phone__phone_number_type_id) AS _hook__reference__phone_number_type,
    CONCAT_WS('~',
      _hook__person__individual,
      'epoch__valid_from|'||person_phone__record_valid_from
    ) AS _pit_hook__person__individual,
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
  person_phone__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND person_phone__record_updated_at BETWEEN @start_ts AND @end_ts
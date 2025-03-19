MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual),
  description 'Business viewpoint of person_phones data: Telephone number and type of a person.',
  column_descriptions (
    person_phone__business_entity_id = 'Business entity identification number. Foreign key to Person.BusinessEntityID.',
    person_phone__phone_number = 'Telephone number identification number.',
    person_phone__phone_number_type_id = 'Kind of phone number. Foreign key to PhoneNumberType.PhoneNumberTypeID.',
    person_phone__modified_date = 'Date when this record was last modified',
    person_phone__record_loaded_at = 'Timestamp when this record was loaded into the system',
    person_phone__record_updated_at = 'Timestamp when this record was last updated',
    person_phone__record_version = 'Version number for this record',
    person_phone__record_valid_from = 'Timestamp from which this record version is valid',
    person_phone__record_valid_to = 'Timestamp until which this record version is valid',
    person_phone__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__person__individual,
    person_phone__business_entity_id,
    person_phone__phone_number,
    person_phone__phone_number_type_id,
    person_phone__modified_date,
    person_phone__record_loaded_at,
    person_phone__record_updated_at,
    person_phone__record_version,
    person_phone__record_valid_from,
    person_phone__record_valid_to,
    person_phone__is_current_record
  FROM dab.bag__adventure_works__person_phones
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__individual,
    NULL AS person_phone__business_entity_id,
    'N/A' AS person_phone__phone_number,
    NULL AS person_phone__phone_number_type_id,
    NULL AS person_phone__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS person_phone__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS person_phone__record_updated_at,
    0 AS person_phone__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS person_phone__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS person_phone__record_valid_to,
    TRUE AS person_phone__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__individual::BLOB,
  person_phone__business_entity_id::BIGINT,
  person_phone__phone_number::TEXT,
  person_phone__phone_number_type_id::BIGINT,
  person_phone__modified_date::DATE,
  person_phone__record_loaded_at::TIMESTAMP,
  person_phone__record_updated_at::TIMESTAMP,
  person_phone__record_version::TEXT,
  person_phone__record_valid_from::TIMESTAMP,
  person_phone__record_valid_to::TIMESTAMP,
  person_phone__is_current_record::BOOLEAN
FROM cte__final
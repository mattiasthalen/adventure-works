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

SELECT
  *
  EXCLUDE (_hook__person__individual, _hook__reference__phone_number_type)
FROM dab.bag__adventure_works__person_phones
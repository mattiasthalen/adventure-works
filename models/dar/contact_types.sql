MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__contact_type),
  description 'Business viewpoint of contact_types data: Lookup table containing the types of business entity contacts.',
  column_descriptions (
    contact_type__contact_type_id = 'Primary key for ContactType records.',
    contact_type__name = 'Contact type description.',
    contact_type__modified_date = 'Date when this record was last modified',
    contact_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    contact_type__record_updated_at = 'Timestamp when this record was last updated',
    contact_type__record_version = 'Version number for this record',
    contact_type__record_valid_from = 'Timestamp from which this record version is valid',
    contact_type__record_valid_to = 'Timestamp until which this record version is valid',
    contact_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__contact_type)
FROM dab.bag__adventure_works__contact_types
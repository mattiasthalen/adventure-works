MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__phone_number_type),
  description 'Business viewpoint of phone_number_types data: Type of phone number of a person.',
  column_descriptions (
    phone_number_type__phone_number_type_id = 'Primary key for telephone number type records.',
    phone_number_type__name = 'Name of the telephone number type.',
    phone_number_type__modified_date = 'Date when this record was last modified',
    phone_number_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    phone_number_type__record_updated_at = 'Timestamp when this record was last updated',
    phone_number_type__record_version = 'Version number for this record',
    phone_number_type__record_valid_from = 'Timestamp from which this record version is valid',
    phone_number_type__record_valid_to = 'Timestamp until which this record version is valid',
    phone_number_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__phone_number_type)
FROM dab.bag__adventure_works__phone_number_types
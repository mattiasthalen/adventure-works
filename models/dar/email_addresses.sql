MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__individual),
  description 'Business viewpoint of email_addresses data: Where to send a person email.',
  column_descriptions (
    email_address__business_entity_id = 'Primary key. Person associated with this email address. Foreign key to Person.BusinessEntityID.',
    email_address__email_address_id = 'Primary key. ID of this email address.',
    email_address__email = 'E-mail address for the person.',
    email_address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    email_address__modified_date = 'Date when this record was last modified',
    email_address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    email_address__record_updated_at = 'Timestamp when this record was last updated',
    email_address__record_version = 'Version number for this record',
    email_address__record_valid_from = 'Timestamp from which this record version is valid',
    email_address__record_valid_to = 'Timestamp until which this record version is valid',
    email_address__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__person__individual, _hook__email_address)
FROM dab.bag__adventure_works__email_addresses
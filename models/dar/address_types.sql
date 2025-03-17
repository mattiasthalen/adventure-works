MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__address_type),
  description 'Business viewpoint of address_types data: Types of addresses stored in the Address table.',
  column_descriptions (
    address_type__address_type_id = 'Primary key for AddressType records.',
    address_type__name = 'Address type description. For example, Billing, Home, or Shipping.',
    address_type__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    address_type__modified_date = 'Date when this record was last modified',
    address_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    address_type__record_updated_at = 'Timestamp when this record was last updated',
    address_type__record_version = 'Version number for this record',
    address_type__record_valid_from = 'Timestamp from which this record version is valid',
    address_type__record_valid_to = 'Timestamp until which this record version is valid',
    address_type__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__reference__address_type)
FROM dab.bag__adventure_works__address_types
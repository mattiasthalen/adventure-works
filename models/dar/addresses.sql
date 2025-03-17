MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__address),
  description 'Business viewpoint of addresses data: Street address information for customers, employees, and vendors.',
  column_descriptions (
    address__address_id = 'Primary key for Address records.',
    address__address_line1 = 'First street address line.',
    address__city = 'Name of the city.',
    address__state_province_id = 'Unique identification number for the state or province. Foreign key to StateProvince table.',
    address__postal_code = 'Postal code for the street address.',
    address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    address__address_line2 = 'Second street address line.',
    address__modified_date = 'Date when this record was last modified',
    address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    address__record_updated_at = 'Timestamp when this record was last updated',
    address__record_version = 'Version number for this record',
    address__record_valid_from = 'Timestamp from which this record version is valid',
    address__record_valid_to = 'Timestamp until which this record version is valid',
    address__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__reference__state_province)
FROM dab.bag__adventure_works__addresses
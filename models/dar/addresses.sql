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

WITH cte__source AS (
  SELECT
    _pit_hook__address,
    address__address_id,
    address__address_line1,
    address__city,
    address__state_province_id,
    address__postal_code,
    address__rowguid,
    address__address_line2,
    address__modified_date,
    address__record_loaded_at,
    address__record_updated_at,
    address__record_version,
    address__record_valid_from,
    address__record_valid_to,
    address__is_current_record
  FROM dab.bag__adventure_works__addresses
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__address,
    NULL AS address__address_id,
    'N/A' AS address__address_line1,
    'N/A' AS address__city,
    NULL AS address__state_province_id,
    'N/A' AS address__postal_code,
    'N/A' AS address__rowguid,
    'N/A' AS address__address_line2,
    NULL AS address__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS address__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS address__record_updated_at,
    0 AS address__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS address__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS address__record_valid_to,
    TRUE AS address__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__address::BLOB,
  address__address_id::BIGINT,
  address__address_line1::TEXT,
  address__city::TEXT,
  address__state_province_id::BIGINT,
  address__postal_code::TEXT,
  address__rowguid::TEXT,
  address__address_line2::TEXT,
  address__modified_date::DATE,
  address__record_loaded_at::TIMESTAMP,
  address__record_updated_at::TIMESTAMP,
  address__record_version::TEXT,
  address__record_valid_from::TIMESTAMP,
  address__record_valid_to::TIMESTAMP,
  address__is_current_record::BOOLEAN
FROM cte__final
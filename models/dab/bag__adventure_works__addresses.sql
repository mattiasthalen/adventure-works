MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__address
  ),
  tags hook,
  grain (_pit_hook__address, _hook__address),
  description 'Hook viewpoint of addresses data: Street address information for customers, employees, and vendors.',
  references (_hook__reference__state_province),
  column_descriptions (
    address__address_id = 'Primary key for Address records.',
    address__address_line1 = 'First street address line.',
    address__city = 'Name of the city.',
    address__state_province_id = 'Unique identification number for the state or province. Foreign key to StateProvince table.',
    address__postal_code = 'Postal code for the street address.',
    address__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    address__address_line2 = 'Second street address line.',
    address__record_loaded_at = 'Timestamp when this record was loaded into the system',
    address__record_updated_at = 'Timestamp when this record was last updated',
    address__record_version = 'Version number for this record',
    address__record_valid_from = 'Timestamp from which this record version is valid',
    address__record_valid_to = 'Timestamp until which this record version is valid',
    address__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__address = 'Reference hook to address',
    _hook__reference__state_province = 'Reference hook to state_province reference',
    _pit_hook__address = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    address_id AS address__address_id,
    address_line1 AS address__address_line1,
    city AS address__city,
    state_province_id AS address__state_province_id,
    postal_code AS address__postal_code,
    rowguid AS address__rowguid,
    address_line2 AS address__address_line2,
    modified_date AS address__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address__record_loaded_at
  FROM das.raw__adventure_works__addresses
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address__address_id ORDER BY address__record_loaded_at) AS address__record_version,
    CASE
      WHEN address__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE address__record_loaded_at
    END AS address__record_valid_from,
    COALESCE(
      LEAD(address__record_loaded_at) OVER (PARTITION BY address__address_id ORDER BY address__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS address__record_valid_to,
    address__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS address__is_current_record,
    CASE
      WHEN address__is_current_record
      THEN address__record_loaded_at
      ELSE address__record_valid_to
    END AS address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('address__adventure_works|', address__address_id) AS _hook__address,
    CONCAT('reference__state_province__adventure_works|', address__state_province_id) AS _hook__reference__state_province,
    CONCAT_WS('~',
      _hook__address,
      'epoch__valid_from|'||address__record_valid_from
    ) AS _pit_hook__address,
    *
  FROM validity
)
SELECT
  _pit_hook__address::BLOB,
  _hook__address::BLOB,
  _hook__reference__state_province::BLOB,
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
  address__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND address__record_updated_at BETWEEN @start_ts AND @end_ts
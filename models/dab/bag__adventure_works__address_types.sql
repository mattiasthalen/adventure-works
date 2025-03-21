MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__address_type
  ),
  tags hook,
  grain (_pit_hook__reference__address_type, _hook__reference__address_type),
  description 'Hook viewpoint of address_types data: Types of addresses stored in the Address table.',
  column_descriptions (
    address_type__address_type_id = 'Primary key for AddressType records.',
    address_type__name = 'Address type description. For example, Billing, Home, or Shipping.',
    address_type__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    address_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    address_type__record_updated_at = 'Timestamp when this record was last updated',
    address_type__record_version = 'Version number for this record',
    address_type__record_valid_from = 'Timestamp from which this record version is valid',
    address_type__record_valid_to = 'Timestamp until which this record version is valid',
    address_type__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__address_type = 'Reference hook to address_type reference',
    _pit_hook__reference__address_type = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    address_type_id AS address_type__address_type_id,
    name AS address_type__name,
    rowguid AS address_type__rowguid,
    modified_date AS address_type__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address_type__record_loaded_at
  FROM das.raw__adventure_works__address_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_type__address_type_id ORDER BY address_type__record_loaded_at) AS address_type__record_version,
    CASE
      WHEN address_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE address_type__record_loaded_at
    END AS address_type__record_valid_from,
    COALESCE(
      LEAD(address_type__record_loaded_at) OVER (PARTITION BY address_type__address_type_id ORDER BY address_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS address_type__record_valid_to,
    address_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS address_type__is_current_record,
    CASE
      WHEN address_type__is_current_record
      THEN address_type__record_loaded_at
      ELSE address_type__record_valid_to
    END AS address_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__address_type__adventure_works|', address_type__address_type_id) AS _hook__reference__address_type,
    CONCAT_WS('~',
      _hook__reference__address_type,
      'epoch__valid_from|'||address_type__record_valid_from
    ) AS _pit_hook__reference__address_type,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__address_type::BLOB,
  _hook__reference__address_type::BLOB,
  address_type__address_type_id::BIGINT,
  address_type__name::TEXT,
  address_type__rowguid::TEXT,
  address_type__modified_date::DATE,
  address_type__record_loaded_at::TIMESTAMP,
  address_type__record_updated_at::TIMESTAMP,
  address_type__record_version::TEXT,
  address_type__record_valid_from::TIMESTAMP,
  address_type__record_valid_to::TIMESTAMP,
  address_type__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND address_type__record_updated_at BETWEEN @start_ts AND @end_ts
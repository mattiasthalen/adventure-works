MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__contact_type
  ),
  tags hook,
  grain (_pit_hook__reference__contact_type, _hook__reference__contact_type),
  description 'Hook viewpoint of contact_types data: Lookup table containing the types of business entity contacts.',
  column_descriptions (
    contact_type__contact_type_id = 'Primary key for ContactType records.',
    contact_type__name = 'Contact type description.',
    contact_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    contact_type__record_updated_at = 'Timestamp when this record was last updated',
    contact_type__record_version = 'Version number for this record',
    contact_type__record_valid_from = 'Timestamp from which this record version is valid',
    contact_type__record_valid_to = 'Timestamp until which this record version is valid',
    contact_type__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__contact_type = 'Reference hook to contact_type reference',
    _pit_hook__reference__contact_type = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    contact_type_id AS contact_type__contact_type_id,
    name AS contact_type__name,
    modified_date AS contact_type__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS contact_type__record_loaded_at
  FROM das.raw__adventure_works__contact_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY contact_type__contact_type_id ORDER BY contact_type__record_loaded_at) AS contact_type__record_version,
    CASE
      WHEN contact_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE contact_type__record_loaded_at
    END AS contact_type__record_valid_from,
    COALESCE(
      LEAD(contact_type__record_loaded_at) OVER (PARTITION BY contact_type__contact_type_id ORDER BY contact_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS contact_type__record_valid_to,
    contact_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS contact_type__is_current_record,
    CASE
      WHEN contact_type__is_current_record
      THEN contact_type__record_loaded_at
      ELSE contact_type__record_valid_to
    END AS contact_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__contact_type__adventure_works|', contact_type__contact_type_id) AS _hook__reference__contact_type,
    CONCAT_WS('~',
      _hook__reference__contact_type,
      'epoch__valid_from|'||contact_type__record_valid_from
    ) AS _pit_hook__reference__contact_type,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__contact_type::BLOB,
  _hook__reference__contact_type::BLOB,
  contact_type__contact_type_id::BIGINT,
  contact_type__name::TEXT,
  contact_type__modified_date::DATE,
  contact_type__record_loaded_at::TIMESTAMP,
  contact_type__record_updated_at::TIMESTAMP,
  contact_type__record_version::TEXT,
  contact_type__record_valid_from::TIMESTAMP,
  contact_type__record_valid_to::TIMESTAMP,
  contact_type__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND contact_type__record_updated_at BETWEEN @start_ts AND @end_ts
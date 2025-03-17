MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__phone_number_type
  ),
  tags hook,
  grain (_pit_hook__reference__phone_number_type, _hook__reference__phone_number_type),
  description 'Hook viewpoint of phone_number_types data: Type of phone number of a person.',
  column_descriptions (
    phone_number_type__phone_number_type_id = 'Primary key for telephone number type records.',
    phone_number_type__name = 'Name of the telephone number type.',
    phone_number_type__record_loaded_at = 'Timestamp when this record was loaded into the system',
    phone_number_type__record_updated_at = 'Timestamp when this record was last updated',
    phone_number_type__record_version = 'Version number for this record',
    phone_number_type__record_valid_from = 'Timestamp from which this record version is valid',
    phone_number_type__record_valid_to = 'Timestamp until which this record version is valid',
    phone_number_type__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__phone_number_type = 'Reference hook to phone_number_type reference',
    _pit_hook__reference__phone_number_type = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    phone_number_type_id AS phone_number_type__phone_number_type_id,
    name AS phone_number_type__name,
    modified_date AS phone_number_type__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS phone_number_type__record_loaded_at
  FROM das.raw__adventure_works__phone_number_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY phone_number_type__phone_number_type_id ORDER BY phone_number_type__record_loaded_at) AS phone_number_type__record_version,
    CASE
      WHEN phone_number_type__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE phone_number_type__record_loaded_at
    END AS phone_number_type__record_valid_from,
    COALESCE(
      LEAD(phone_number_type__record_loaded_at) OVER (PARTITION BY phone_number_type__phone_number_type_id ORDER BY phone_number_type__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS phone_number_type__record_valid_to,
    phone_number_type__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS phone_number_type__is_current_record,
    CASE
      WHEN phone_number_type__is_current_record
      THEN phone_number_type__record_loaded_at
      ELSE phone_number_type__record_valid_to
    END AS phone_number_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__phone_number_type__adventure_works|', phone_number_type__phone_number_type_id) AS _hook__reference__phone_number_type,
    CONCAT_WS('~',
      _hook__reference__phone_number_type,
      'epoch__valid_from|'||phone_number_type__record_valid_from
    ) AS _pit_hook__reference__phone_number_type,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__phone_number_type::BLOB,
  _hook__reference__phone_number_type::BLOB,
  phone_number_type__phone_number_type_id::BIGINT,
  phone_number_type__name::TEXT,
  phone_number_type__modified_date::DATE,
  phone_number_type__record_loaded_at::TIMESTAMP,
  phone_number_type__record_updated_at::TIMESTAMP,
  phone_number_type__record_version::TEXT,
  phone_number_type__record_valid_from::TIMESTAMP,
  phone_number_type__record_valid_to::TIMESTAMP,
  phone_number_type__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND phone_number_type__record_updated_at BETWEEN @start_ts AND @end_ts
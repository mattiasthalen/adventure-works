MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column contact_type__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__reference__contact_type, _hook__reference__contact_type)
);

WITH staging AS (
  SELECT
    contact_type_id AS contact_type__contact_type_id,
    name AS contact_type__name,
    modified_date AS contact_type__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS contact_type__record_loaded_at
  FROM bronze.raw__adventure_works__contact_types
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
    CONCAT(
      'reference__contact_type__adventure_works|',
      contact_type__contact_type_id,
      '~epoch__valid_from|',
      contact_type__record_valid_from
    )::BLOB AS _pit_hook__reference__contact_type,
    CONCAT('reference__contact_type__adventure_works|', contact_type__contact_type_id) AS _hook__reference__contact_type,
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
  contact_type__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND contact_type__record_updated_at BETWEEN @start_ts AND @end_ts
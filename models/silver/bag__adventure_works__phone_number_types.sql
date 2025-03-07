MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__phone_number_type,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__phone_number_type, _hook__reference__phone_number_type)
);

WITH staging AS (
  SELECT
    phone_number_type_id AS phone_number_type__phone_number_type_id,
    name AS phone_number_type__name,
    modified_date AS phone_number_type__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS phone_number_type__record_loaded_at
  FROM bronze.raw__adventure_works__phone_number_types
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
    CONCAT(
      'reference__phone_number_type__adventure_works|',
      phone_number_type__phone_number_type_id,
      '~epoch|valid_from|',
      phone_number_type__record_valid_from
    )::BLOB AS _pit_hook__reference__phone_number_type,
    CONCAT('reference__phone_number_type__adventure_works|', phone_number_type__phone_number_type_id) AS _hook__reference__phone_number_type,
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
  phone_number_type__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND phone_number_type__record_updated_at BETWEEN @start_ts AND @end_ts
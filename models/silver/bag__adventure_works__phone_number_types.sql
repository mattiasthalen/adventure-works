MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column phone_number_typ__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    phone_number_type_id AS phone_number_typ__phone_number_type_id,
    modified_date AS phone_number_typ__modified_date,
    name AS phone_number_typ__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS phone_number_typ__record_loaded_at
  FROM bronze.raw__adventure_works__phone_number_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY phone_number_typ__phone_number_type_id ORDER BY phone_number_typ__record_loaded_at) AS phone_number_typ__record_version,
    CASE
      WHEN phone_number_typ__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE phone_number_typ__record_loaded_at
    END AS phone_number_typ__record_valid_from,
    COALESCE(
      LEAD(phone_number_typ__record_loaded_at) OVER (PARTITION BY phone_number_typ__phone_number_type_id ORDER BY phone_number_typ__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS phone_number_typ__record_valid_to,
    phone_number_typ__record_valid_to = @max_ts::TIMESTAMP AS phone_number_typ__is_current_record,
    CASE
      WHEN phone_number_typ__is_current_record
      THEN phone_number_typ__record_loaded_at
      ELSE phone_number_typ__record_valid_to
    END AS phone_number_typ__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'phone_number_type|adventure_works|',
      phone_number_typ__phone_number_type_id,
      '~epoch|valid_from|',
      phone_number_typ__record_valid_from
    ) AS _pit_hook__phone_number_type,
    CONCAT('phone_number_type|adventure_works|', phone_number_typ__phone_number_type_id) AS _hook__phone_number_type,
    *
  FROM validity
)
SELECT
  _pit_hook__phone_number_type::BLOB,
  _hook__phone_number_type::BLOB,
  phone_number_typ__phone_number_type_id::BIGINT,
  phone_number_typ__modified_date::VARCHAR,
  phone_number_typ__name::VARCHAR,
  phone_number_typ__record_loaded_at::TIMESTAMP,
  phone_number_typ__record_updated_at::TIMESTAMP,
  phone_number_typ__record_valid_from::TIMESTAMP,
  phone_number_typ__record_valid_to::TIMESTAMP,
  phone_number_typ__record_version::INT,
  phone_number_typ__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND phone_number_typ__record_updated_at BETWEEN @start_ts AND @end_ts
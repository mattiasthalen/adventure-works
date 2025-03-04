MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column contact_typ__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    contact_type_id AS contact_typ__contact_type_id,
    modified_date AS contact_typ__modified_date,
    name AS contact_typ__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS contact_typ__record_loaded_at
  FROM bronze.raw__adventure_works__contact_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY contact_typ__contact_type_id ORDER BY contact_typ__record_loaded_at) AS contact_typ__record_version,
    CASE
      WHEN contact_typ__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE contact_typ__record_loaded_at
    END AS contact_typ__record_valid_from,
    COALESCE(
      LEAD(contact_typ__record_loaded_at) OVER (PARTITION BY contact_typ__contact_type_id ORDER BY contact_typ__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS contact_typ__record_valid_to,
    contact_typ__record_valid_to = @max_ts::TIMESTAMP AS contact_typ__is_current_record,
    CASE
      WHEN contact_typ__is_current_record
      THEN contact_typ__record_loaded_at
      ELSE contact_typ__record_valid_to
    END AS contact_typ__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'contact_type|adventure_works|',
      contact_typ__contact_type_id,
      '~epoch|valid_from|',
      contact_typ__record_valid_from
    ) AS _pit_hook__contact_type,
    CONCAT('contact_type|adventure_works|', contact_typ__contact_type_id) AS _hook__contact_type,
    *
  FROM validity
)
SELECT
  _pit_hook__contact_type::BLOB,
  _hook__contact_type::BLOB,
  contact_typ__contact_type_id::BIGINT,
  contact_typ__modified_date::VARCHAR,
  contact_typ__name::VARCHAR,
  contact_typ__record_loaded_at::TIMESTAMP,
  contact_typ__record_updated_at::TIMESTAMP,
  contact_typ__record_valid_from::TIMESTAMP,
  contact_typ__record_valid_to::TIMESTAMP,
  contact_typ__record_version::INT,
  contact_typ__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND contact_typ__record_updated_at BETWEEN @start_ts AND @end_ts
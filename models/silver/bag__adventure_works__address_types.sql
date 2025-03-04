MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column address_typ__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    address_type_id AS address_typ__address_type_id,
    modified_date AS address_typ__modified_date,
    name AS address_typ__name,
    rowguid AS address_typ__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address_typ__record_loaded_at
  FROM bronze.raw__adventure_works__address_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_typ__address_type_id ORDER BY address_typ__record_loaded_at) AS address_typ__record_version,
    CASE
      WHEN address_typ__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE address_typ__record_loaded_at
    END AS address_typ__record_valid_from,
    COALESCE(
      LEAD(address_typ__record_loaded_at) OVER (PARTITION BY address_typ__address_type_id ORDER BY address_typ__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS address_typ__record_valid_to,
    address_typ__record_valid_to = @max_ts::TIMESTAMP AS address_typ__is_current_record,
    CASE
      WHEN address_typ__is_current_record
      THEN address_typ__record_loaded_at
      ELSE address_typ__record_valid_to
    END AS address_typ__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'address_type|adventure_works|',
      address_typ__address_type_id,
      '~epoch|valid_from|',
      address_typ__record_valid_from
    ) AS _pit_hook__address_type,
    CONCAT('address_type|adventure_works|', address_typ__address_type_id) AS _hook__address_type,
    *
  FROM validity
)
SELECT
  _pit_hook__address_type::BLOB,
  _hook__address_type::BLOB,
  address_typ__address_type_id::BIGINT,
  address_typ__modified_date::VARCHAR,
  address_typ__name::VARCHAR,
  address_typ__rowguid::VARCHAR,
  address_typ__record_loaded_at::TIMESTAMP,
  address_typ__record_updated_at::TIMESTAMP,
  address_typ__record_valid_from::TIMESTAMP,
  address_typ__record_valid_to::TIMESTAMP,
  address_typ__record_version::INT,
  address_typ__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND address_typ__record_updated_at BETWEEN @start_ts AND @end_ts
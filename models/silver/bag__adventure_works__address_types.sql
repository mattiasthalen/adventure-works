MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    address_type_id AS address_type__address_type_id,
    modified_date AS address_type__modified_date,
    name AS address_type__name,
    rowguid AS address_type__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address_type__record_loaded_at
  FROM bronze.raw__adventure_works__address_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address_type__address_type_id ORDER BY address_type__record_loaded_at) AS address_type__record_version,
    CASE
      WHEN address_type__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE address_type__record_loaded_at
    END AS address_type__record_valid_from,
    COALESCE(
      LEAD(address_type__record_loaded_at) OVER (PARTITION BY address_type__address_type_id ORDER BY address_type__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS address_type__record_valid_to,
    address_type__record_valid_to = @max_ts::TIMESTAMP AS address_type__is_current_record,
    CASE
      WHEN address_type__is_current_record
      THEN address_type__record_loaded_at
      ELSE address_type__record_valid_to
    END AS address_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'address_type|adventure_works|',
      address_type__address_type_id,
      '~epoch|valid_from|',
      address_type__record_valid_from
    ) AS _pit_hook__address_type,
    CONCAT('address_type|adventure_works|', address_type__address_type_id) AS _hook__address_type,
    *
  FROM validity
)
SELECT
  _pit_hook__address_type::BLOB,
  _hook__address_type::BLOB,
  address_type__address_type_id::VARCHAR,
  address_type__modified_date::VARCHAR,
  address_type__name::VARCHAR,
  address_type__rowguid::VARCHAR,
  address_type__record_loaded_at::TIMESTAMP,
  address_type__record_version::INT,
  address_type__record_valid_from::TIMESTAMP,
  address_type__record_valid_to::TIMESTAMP,
  address_type__is_current_record::BOOLEAN,
  address_type__record_updated_at::TIMESTAMP
FROM hooks
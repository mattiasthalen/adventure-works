MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    address_id AS address__address_id,
    address_type_id AS address__address_type_id,
    business_entity_id AS address__business_entity_id,
    modified_date AS address__modified_date,
    rowguid AS address__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS address__record_loaded_at
  FROM bronze.raw__adventure_works__business_entity_addresses
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY address__address_id ORDER BY address__record_loaded_at) AS address__record_version,
    CASE
      WHEN address__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE address__record_loaded_at
    END AS address__record_valid_from,
    COALESCE(
      LEAD(address__record_loaded_at) OVER (PARTITION BY address__address_id ORDER BY address__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS address__record_valid_to,
    address__record_valid_to = @max_ts::TIMESTAMP AS address__is_current_record,
    CASE
      WHEN address__is_current_record
      THEN address__record_loaded_at
      ELSE address__record_valid_to
    END AS address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'address|adventure_works|',
      address__address_id,
      '~epoch|valid_from|',
      address__record_valid_from
    ) AS _pit_hook__address,
    CONCAT('address|adventure_works|', address__address_id) AS _hook__address,
    CONCAT('address_type|adventure_works|', address__address_type_id) AS _hook__address_type,
    CONCAT('business_entity|adventure_works|', address__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__address::BLOB,
  _hook__address::BLOB,
  _hook__address_type::BLOB,
  _hook__business_entity::BLOB,
  address__address_id::VARCHAR,
  address__address_type_id::VARCHAR,
  address__business_entity_id::VARCHAR,
  address__modified_date::VARCHAR,
  address__rowguid::VARCHAR,
  address__record_loaded_at::TIMESTAMP,
  address__record_version::INT,
  address__record_valid_from::TIMESTAMP,
  address__record_valid_to::TIMESTAMP,
  address__is_current_record::BOOLEAN,
  address__record_updated_at::TIMESTAMP
FROM hooks
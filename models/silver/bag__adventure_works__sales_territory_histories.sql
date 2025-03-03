MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity__business_entity_id,
    territory_id AS business_entity__territory_id,
    end_date AS business_entity__end_date,
    modified_date AS business_entity__modified_date,
    rowguid AS business_entity__rowguid,
    start_date AS business_entity__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM bronze.raw__adventure_works__sales_territory_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at) AS business_entity__record_version,
    CASE
      WHEN business_entity__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE business_entity__record_loaded_at
    END AS business_entity__record_valid_from,
    COALESCE(
      LEAD(business_entity__record_loaded_at) OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS business_entity__record_valid_to,
    business_entity__record_valid_to = @max_ts::TIMESTAMP AS business_entity__is_current_record,
    CASE
      WHEN business_entity__is_current_record
      THEN business_entity__record_loaded_at
      ELSE business_entity__record_valid_to
    END AS business_entity__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      business_entity__business_entity_id,
      '~epoch|valid_from|',
      business_entity__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity__business_entity_id) AS _hook__business_entity,
    CONCAT('territory|adventure_works|', business_entity__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__territory::BLOB,
  business_entity__business_entity_id::VARCHAR,
  business_entity__territory_id::VARCHAR,
  business_entity__end_date::VARCHAR,
  business_entity__modified_date::VARCHAR,
  business_entity__rowguid::VARCHAR,
  business_entity__start_date::VARCHAR,
  business_entity__record_loaded_at::TIMESTAMP,
  business_entity__record_version::INT,
  business_entity__record_valid_from::TIMESTAMP,
  business_entity__record_valid_to::TIMESTAMP,
  business_entity__is_current_record::BOOLEAN,
  business_entity__record_updated_at::TIMESTAMP
FROM hooks
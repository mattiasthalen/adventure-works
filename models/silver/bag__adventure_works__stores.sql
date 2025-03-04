MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column stor__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS stor__business_entity_id,
    sales_person_id AS stor__sales_person_id,
    demographics AS stor__demographics,
    modified_date AS stor__modified_date,
    name AS stor__name,
    rowguid AS stor__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS stor__record_loaded_at
  FROM bronze.raw__adventure_works__stores
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY stor__business_entity_id ORDER BY stor__record_loaded_at) AS stor__record_version,
    CASE
      WHEN stor__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE stor__record_loaded_at
    END AS stor__record_valid_from,
    COALESCE(
      LEAD(stor__record_loaded_at) OVER (PARTITION BY stor__business_entity_id ORDER BY stor__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS stor__record_valid_to,
    stor__record_valid_to = @max_ts::TIMESTAMP AS stor__is_current_record,
    CASE
      WHEN stor__is_current_record
      THEN stor__record_loaded_at
      ELSE stor__record_valid_to
    END AS stor__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      stor__business_entity_id,
      '~epoch|valid_from|',
      stor__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', stor__business_entity_id) AS _hook__business_entity,
    CONCAT('sales_person|adventure_works|', stor__sales_person_id) AS _hook__sales_person,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__sales_person::BLOB,
  stor__business_entity_id::BIGINT,
  stor__sales_person_id::BIGINT,
  stor__demographics::VARCHAR,
  stor__modified_date::VARCHAR,
  stor__name::VARCHAR,
  stor__rowguid::VARCHAR,
  stor__record_loaded_at::TIMESTAMP,
  stor__record_updated_at::TIMESTAMP,
  stor__record_valid_from::TIMESTAMP,
  stor__record_valid_to::TIMESTAMP,
  stor__record_version::INT,
  stor__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND stor__record_updated_at BETWEEN @start_ts AND @end_ts
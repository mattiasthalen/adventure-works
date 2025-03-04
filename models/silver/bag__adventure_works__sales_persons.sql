MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_person__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS sales_person__business_entity_id,
    territory_id AS sales_person__territory_id,
    bonus AS sales_person__bonus,
    commission_pct AS sales_person__commission_pct,
    modified_date AS sales_person__modified_date,
    rowguid AS sales_person__rowguid,
    sales_last_year AS sales_person__sales_last_year,
    sales_quota AS sales_person__sales_quota,
    sales_ytd AS sales_person__sales_ytd,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_person__record_loaded_at
  FROM bronze.raw__adventure_works__sales_persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_person__business_entity_id ORDER BY sales_person__record_loaded_at) AS sales_person__record_version,
    CASE
      WHEN sales_person__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_person__record_loaded_at
    END AS sales_person__record_valid_from,
    COALESCE(
      LEAD(sales_person__record_loaded_at) OVER (PARTITION BY sales_person__business_entity_id ORDER BY sales_person__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_person__record_valid_to,
    sales_person__record_valid_to = @max_ts::TIMESTAMP AS sales_person__is_current_record,
    CASE
      WHEN sales_person__is_current_record
      THEN sales_person__record_loaded_at
      ELSE sales_person__record_valid_to
    END AS sales_person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      sales_person__business_entity_id,
      '~epoch|valid_from|',
      sales_person__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', sales_person__business_entity_id) AS _hook__business_entity,
    CONCAT('territory|adventure_works|', sales_person__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__territory::BLOB,
  sales_person__business_entity_id::BIGINT,
  sales_person__territory_id::BIGINT,
  sales_person__bonus::DOUBLE,
  sales_person__commission_pct::DOUBLE,
  sales_person__modified_date::VARCHAR,
  sales_person__rowguid::VARCHAR,
  sales_person__sales_last_year::DOUBLE,
  sales_person__sales_quota::DOUBLE,
  sales_person__sales_ytd::DOUBLE,
  sales_person__record_loaded_at::TIMESTAMP,
  sales_person__record_updated_at::TIMESTAMP,
  sales_person__record_valid_from::TIMESTAMP,
  sales_person__record_valid_to::TIMESTAMP,
  sales_person__record_version::INT,
  sales_person__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND sales_person__record_updated_at BETWEEN @start_ts AND @end_ts
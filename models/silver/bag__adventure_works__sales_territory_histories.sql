MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_territory_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS sales_territory_histori__business_entity_id,
    territory_id AS sales_territory_histori__territory_id,
    end_date AS sales_territory_histori__end_date,
    modified_date AS sales_territory_histori__modified_date,
    rowguid AS sales_territory_histori__rowguid,
    start_date AS sales_territory_histori__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_territory_histori__record_loaded_at
  FROM bronze.raw__adventure_works__sales_territory_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_territory_histori__business_entity_id ORDER BY sales_territory_histori__record_loaded_at) AS sales_territory_histori__record_version,
    CASE
      WHEN sales_territory_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_territory_histori__record_loaded_at
    END AS sales_territory_histori__record_valid_from,
    COALESCE(
      LEAD(sales_territory_histori__record_loaded_at) OVER (PARTITION BY sales_territory_histori__business_entity_id ORDER BY sales_territory_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_territory_histori__record_valid_to,
    sales_territory_histori__record_valid_to = @max_ts::TIMESTAMP AS sales_territory_histori__is_current_record,
    CASE
      WHEN sales_territory_histori__is_current_record
      THEN sales_territory_histori__record_loaded_at
      ELSE sales_territory_histori__record_valid_to
    END AS sales_territory_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      sales_territory_histori__business_entity_id,
      '~epoch|valid_from|',
      sales_territory_histori__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', sales_territory_histori__business_entity_id) AS _hook__business_entity,
    CONCAT('territory|adventure_works|', sales_territory_histori__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  _hook__territory::BLOB,
  sales_territory_histori__business_entity_id::BIGINT,
  sales_territory_histori__territory_id::BIGINT,
  sales_territory_histori__end_date::VARCHAR,
  sales_territory_histori__modified_date::VARCHAR,
  sales_territory_histori__rowguid::VARCHAR,
  sales_territory_histori__start_date::VARCHAR,
  sales_territory_histori__record_loaded_at::TIMESTAMP,
  sales_territory_histori__record_updated_at::TIMESTAMP,
  sales_territory_histori__record_valid_from::TIMESTAMP,
  sales_territory_histori__record_valid_to::TIMESTAMP,
  sales_territory_histori__record_version::INT,
  sales_territory_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND sales_territory_histori__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_territori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    territory_id AS sales_territori__territory_id,
    cost_last_year AS sales_territori__cost_last_year,
    cost_ytd AS sales_territori__cost_ytd,
    country_region_code AS sales_territori__country_region_code,
    group AS sales_territori__group,
    modified_date AS sales_territori__modified_date,
    name AS sales_territori__name,
    rowguid AS sales_territori__rowguid,
    sales_last_year AS sales_territori__sales_last_year,
    sales_ytd AS sales_territori__sales_ytd,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_territori__record_loaded_at
  FROM bronze.raw__adventure_works__sales_territories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_territori__territory_id ORDER BY sales_territori__record_loaded_at) AS sales_territori__record_version,
    CASE
      WHEN sales_territori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_territori__record_loaded_at
    END AS sales_territori__record_valid_from,
    COALESCE(
      LEAD(sales_territori__record_loaded_at) OVER (PARTITION BY sales_territori__territory_id ORDER BY sales_territori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_territori__record_valid_to,
    sales_territori__record_valid_to = @max_ts::TIMESTAMP AS sales_territori__is_current_record,
    CASE
      WHEN sales_territori__is_current_record
      THEN sales_territori__record_loaded_at
      ELSE sales_territori__record_valid_to
    END AS sales_territori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'territory|adventure_works|',
      sales_territori__territory_id,
      '~epoch|valid_from|',
      sales_territori__record_valid_from
    ) AS _pit_hook__territory,
    CONCAT('territory|adventure_works|', sales_territori__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__territory::BLOB,
  _hook__territory::BLOB,
  sales_territori__territory_id::BIGINT,
  sales_territori__cost_last_year::DOUBLE,
  sales_territori__cost_ytd::DOUBLE,
  sales_territori__country_region_code::VARCHAR,
  sales_territori__group::VARCHAR,
  sales_territori__modified_date::VARCHAR,
  sales_territori__name::VARCHAR,
  sales_territori__rowguid::VARCHAR,
  sales_territori__sales_last_year::DOUBLE,
  sales_territori__sales_ytd::DOUBLE,
  sales_territori__record_loaded_at::TIMESTAMP,
  sales_territori__record_updated_at::TIMESTAMP,
  sales_territori__record_valid_from::TIMESTAMP,
  sales_territori__record_valid_to::TIMESTAMP,
  sales_territori__record_version::INT,
  sales_territori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND sales_territori__record_updated_at BETWEEN @start_ts AND @end_ts
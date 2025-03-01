MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column ship_method__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    ship_method_id,
    modified_date AS ship_method__modified_date,
    name AS ship_method__name,
    rowguid AS ship_method__rowguid,
    ship_base AS ship_method__ship_base,
    ship_rate AS ship_method__ship_rate,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS ship_method__record_loaded_at
  FROM bronze.raw__adventure_works__ship_methods
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ship_method_id ORDER BY ship_method__record_loaded_at) AS ship_method__record_version,
    CASE
      WHEN ship_method__record_version = 1
      THEN @MIN_TS::TIMESTAMP
      ELSE ship_method__record_loaded_at
    END AS ship_method__record_valid_from,
    COALESCE(
      LEAD(ship_method__record_loaded_at) OVER (PARTITION BY ship_method_id ORDER BY ship_method__record_loaded_at),
      @MAX_TS::TIMESTAMP
    ) AS ship_method__record_valid_to,
    ship_method__record_valid_to = @MAX_TS::TIMESTAMP AS ship_method__is_current_record,
    CASE
      WHEN ship_method__is_current_record
      THEN ship_method__record_loaded_at
      ELSE ship_method__record_valid_to
    END AS ship_method__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'ship_method|adventure_works|',
      ship_method_id,
      '~epoch|valid_from|',
      ship_method__record_valid_from
    )::BLOB AS _pit_hook__ship_method,
    CONCAT('ship_method|adventure_works|', ship_method_id)::BLOB AS _hook__ship_method,
    *
  FROM validity
)
SELECT
  *
FROM hooks
WHERE 1 = 1
AND ship_method__record_updated_at BETWEEN @start_ts AND @end_ts
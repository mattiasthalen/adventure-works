MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column unit_measur__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    unit_measure_code AS unit_measur__unit_measure_code,
    modified_date AS unit_measur__modified_date,
    name AS unit_measur__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS unit_measur__record_loaded_at
  FROM bronze.raw__adventure_works__unit_measures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY unit_measur__unit_measure_code ORDER BY unit_measur__record_loaded_at) AS unit_measur__record_version,
    CASE
      WHEN unit_measur__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE unit_measur__record_loaded_at
    END AS unit_measur__record_valid_from,
    COALESCE(
      LEAD(unit_measur__record_loaded_at) OVER (PARTITION BY unit_measur__unit_measure_code ORDER BY unit_measur__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS unit_measur__record_valid_to,
    unit_measur__record_valid_to = @max_ts::TIMESTAMP AS unit_measur__is_current_record,
    CASE
      WHEN unit_measur__is_current_record
      THEN unit_measur__record_loaded_at
      ELSE unit_measur__record_valid_to
    END AS unit_measur__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'unit_measure_code|adventure_works|',
      unit_measur__unit_measure_code,
      '~epoch|valid_from|',
      unit_measur__record_valid_from
    ) AS _pit_hook__unit_measure_code,
    CONCAT('unit_measure_code|adventure_works|', unit_measur__unit_measure_code) AS _hook__unit_measure_code,
    *
  FROM validity
)
SELECT
  _pit_hook__unit_measure_code::BLOB,
  _hook__unit_measure_code::BLOB,
  unit_measur__unit_measure_code::VARCHAR,
  unit_measur__modified_date::VARCHAR,
  unit_measur__name::VARCHAR,
  unit_measur__record_loaded_at::TIMESTAMP,
  unit_measur__record_updated_at::TIMESTAMP,
  unit_measur__record_valid_from::TIMESTAMP,
  unit_measur__record_valid_to::TIMESTAMP,
  unit_measur__record_version::INT,
  unit_measur__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND unit_measur__record_updated_at BETWEEN @start_ts AND @end_ts
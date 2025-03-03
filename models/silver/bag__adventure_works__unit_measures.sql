MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    unit_measure_code AS unit_measure_code__unit_measure_code,
    modified_date AS unit_measure_code__modified_date,
    name AS unit_measure_code__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS unit_measure_code__record_loaded_at
  FROM bronze.raw__adventure_works__unit_measures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY unit_measure_code__unit_measure_code ORDER BY unit_measure_code__record_loaded_at) AS unit_measure_code__record_version,
    CASE
      WHEN unit_measure_code__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE unit_measure_code__record_loaded_at
    END AS unit_measure_code__record_valid_from,
    COALESCE(
      LEAD(unit_measure_code__record_loaded_at) OVER (PARTITION BY unit_measure_code__unit_measure_code ORDER BY unit_measure_code__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS unit_measure_code__record_valid_to,
    unit_measure_code__record_valid_to = @max_ts::TIMESTAMP AS unit_measure_code__is_current_record,
    CASE
      WHEN unit_measure_code__is_current_record
      THEN unit_measure_code__record_loaded_at
      ELSE unit_measure_code__record_valid_to
    END AS unit_measure_code__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'unit_measure_code|adventure_works|',
      unit_measure_code__unit_measure_code,
      '~epoch|valid_from|',
      unit_measure_code__record_valid_from
    ) AS _pit_hook__unit_measure_code,
    CONCAT('unit_measure_code|adventure_works|', unit_measure_code__unit_measure_code) AS _hook__unit_measure_code,
    *
  FROM validity
)
SELECT
  _pit_hook__unit_measure_code::BLOB,
  _hook__unit_measure_code::BLOB,
  unit_measure_code__unit_measure_code::VARCHAR,
  unit_measure_code__modified_date::VARCHAR,
  unit_measure_code__name::VARCHAR,
  unit_measure_code__record_loaded_at::TIMESTAMP,
  unit_measure_code__record_version::INT,
  unit_measure_code__record_valid_from::TIMESTAMP,
  unit_measure_code__record_valid_to::TIMESTAMP,
  unit_measure_code__is_current_record::BOOLEAN,
  unit_measure_code__record_updated_at::TIMESTAMP
FROM hooks
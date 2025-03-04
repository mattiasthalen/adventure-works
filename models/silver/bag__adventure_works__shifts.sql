MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column shift__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    shift_id AS shift__shift_id,
    end_time AS shift__end_time,
    modified_date AS shift__modified_date,
    name AS shift__name,
    start_time AS shift__start_time,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shift__record_loaded_at
  FROM bronze.raw__adventure_works__shifts
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shift__shift_id ORDER BY shift__record_loaded_at) AS shift__record_version,
    CASE
      WHEN shift__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE shift__record_loaded_at
    END AS shift__record_valid_from,
    COALESCE(
      LEAD(shift__record_loaded_at) OVER (PARTITION BY shift__shift_id ORDER BY shift__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS shift__record_valid_to,
    shift__record_valid_to = @max_ts::TIMESTAMP AS shift__is_current_record,
    CASE
      WHEN shift__is_current_record
      THEN shift__record_loaded_at
      ELSE shift__record_valid_to
    END AS shift__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'shift|adventure_works|',
      shift__shift_id,
      '~epoch|valid_from|',
      shift__record_valid_from
    ) AS _pit_hook__shift,
    CONCAT('shift|adventure_works|', shift__shift_id) AS _hook__shift,
    *
  FROM validity
)
SELECT
  _pit_hook__shift::BLOB,
  _hook__shift::BLOB,
  shift__shift_id::BIGINT,
  shift__end_time::VARCHAR,
  shift__modified_date::VARCHAR,
  shift__name::VARCHAR,
  shift__start_time::VARCHAR,
  shift__record_loaded_at::TIMESTAMP,
  shift__record_updated_at::TIMESTAMP,
  shift__record_valid_from::TIMESTAMP,
  shift__record_valid_to::TIMESTAMP,
  shift__record_version::INT,
  shift__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND shift__record_updated_at BETWEEN @start_ts AND @end_ts
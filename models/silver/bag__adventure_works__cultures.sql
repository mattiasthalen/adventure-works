MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column cultur__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    culture_id AS cultur__culture_id,
    modified_date AS cultur__modified_date,
    name AS cultur__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS cultur__record_loaded_at
  FROM bronze.raw__adventure_works__cultures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cultur__culture_id ORDER BY cultur__record_loaded_at) AS cultur__record_version,
    CASE
      WHEN cultur__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE cultur__record_loaded_at
    END AS cultur__record_valid_from,
    COALESCE(
      LEAD(cultur__record_loaded_at) OVER (PARTITION BY cultur__culture_id ORDER BY cultur__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS cultur__record_valid_to,
    cultur__record_valid_to = @max_ts::TIMESTAMP AS cultur__is_current_record,
    CASE
      WHEN cultur__is_current_record
      THEN cultur__record_loaded_at
      ELSE cultur__record_valid_to
    END AS cultur__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'culture|adventure_works|',
      cultur__culture_id,
      '~epoch|valid_from|',
      cultur__record_valid_from
    ) AS _pit_hook__culture,
    CONCAT('culture|adventure_works|', cultur__culture_id) AS _hook__culture,
    *
  FROM validity
)
SELECT
  _pit_hook__culture::BLOB,
  _hook__culture::BLOB,
  cultur__culture_id::VARCHAR,
  cultur__modified_date::VARCHAR,
  cultur__name::VARCHAR,
  cultur__record_loaded_at::TIMESTAMP,
  cultur__record_updated_at::TIMESTAMP,
  cultur__record_valid_from::TIMESTAMP,
  cultur__record_valid_to::TIMESTAMP,
  cultur__record_version::INT,
  cultur__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND cultur__record_updated_at BETWEEN @start_ts AND @end_ts
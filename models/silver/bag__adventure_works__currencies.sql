MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column currenci__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    currency_code AS currenci__currency_code,
    modified_date AS currenci__modified_date,
    name AS currenci__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currenci__record_loaded_at
  FROM bronze.raw__adventure_works__currencies
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currenci__currency_code ORDER BY currenci__record_loaded_at) AS currenci__record_version,
    CASE
      WHEN currenci__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE currenci__record_loaded_at
    END AS currenci__record_valid_from,
    COALESCE(
      LEAD(currenci__record_loaded_at) OVER (PARTITION BY currenci__currency_code ORDER BY currenci__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS currenci__record_valid_to,
    currenci__record_valid_to = @max_ts::TIMESTAMP AS currenci__is_current_record,
    CASE
      WHEN currenci__is_current_record
      THEN currenci__record_loaded_at
      ELSE currenci__record_valid_to
    END AS currenci__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency_code|adventure_works|',
      currenci__currency_code,
      '~epoch|valid_from|',
      currenci__record_valid_from
    ) AS _pit_hook__currency_code,
    CONCAT('currency_code|adventure_works|', currenci__currency_code) AS _hook__currency_code,
    *
  FROM validity
)
SELECT
  _pit_hook__currency_code::BLOB,
  _hook__currency_code::BLOB,
  currenci__currency_code::VARCHAR,
  currenci__modified_date::VARCHAR,
  currenci__name::VARCHAR,
  currenci__record_loaded_at::TIMESTAMP,
  currenci__record_updated_at::TIMESTAMP,
  currenci__record_valid_from::TIMESTAMP,
  currenci__record_valid_to::TIMESTAMP,
  currenci__record_version::INT,
  currenci__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND currenci__record_updated_at BETWEEN @start_ts AND @end_ts
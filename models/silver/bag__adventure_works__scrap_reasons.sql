MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column scrap_reason__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    scrap_reason_id AS scrap_reason__scrap_reason_id,
    modified_date AS scrap_reason__modified_date,
    name AS scrap_reason__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS scrap_reason__record_loaded_at
  FROM bronze.raw__adventure_works__scrap_reasons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY scrap_reason__scrap_reason_id ORDER BY scrap_reason__record_loaded_at) AS scrap_reason__record_version,
    CASE
      WHEN scrap_reason__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE scrap_reason__record_loaded_at
    END AS scrap_reason__record_valid_from,
    COALESCE(
      LEAD(scrap_reason__record_loaded_at) OVER (PARTITION BY scrap_reason__scrap_reason_id ORDER BY scrap_reason__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS scrap_reason__record_valid_to,
    scrap_reason__record_valid_to = @max_ts::TIMESTAMP AS scrap_reason__is_current_record,
    CASE
      WHEN scrap_reason__is_current_record
      THEN scrap_reason__record_loaded_at
      ELSE scrap_reason__record_valid_to
    END AS scrap_reason__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'scrap_reason|adventure_works|',
      scrap_reason__scrap_reason_id,
      '~epoch|valid_from|',
      scrap_reason__record_valid_from
    ) AS _pit_hook__scrap_reason,
    CONCAT('scrap_reason|adventure_works|', scrap_reason__scrap_reason_id) AS _hook__scrap_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__scrap_reason::BLOB,
  _hook__scrap_reason::BLOB,
  scrap_reason__scrap_reason_id::BIGINT,
  scrap_reason__modified_date::VARCHAR,
  scrap_reason__name::VARCHAR,
  scrap_reason__record_loaded_at::TIMESTAMP,
  scrap_reason__record_updated_at::TIMESTAMP,
  scrap_reason__record_valid_from::TIMESTAMP,
  scrap_reason__record_valid_to::TIMESTAMP,
  scrap_reason__record_version::INT,
  scrap_reason__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND scrap_reason__record_updated_at BETWEEN @start_ts AND @end_ts
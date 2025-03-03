MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    illustration_id AS illustration__illustration_id,
    diagram AS illustration__diagram,
    modified_date AS illustration__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS illustration__record_loaded_at
  FROM bronze.raw__adventure_works__illustrations
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY illustration__illustration_id ORDER BY illustration__record_loaded_at) AS illustration__record_version,
    CASE
      WHEN illustration__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE illustration__record_loaded_at
    END AS illustration__record_valid_from,
    COALESCE(
      LEAD(illustration__record_loaded_at) OVER (PARTITION BY illustration__illustration_id ORDER BY illustration__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS illustration__record_valid_to,
    illustration__record_valid_to = @max_ts::TIMESTAMP AS illustration__is_current_record,
    CASE
      WHEN illustration__is_current_record
      THEN illustration__record_loaded_at
      ELSE illustration__record_valid_to
    END AS illustration__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'illustration|adventure_works|',
      illustration__illustration_id,
      '~epoch|valid_from|',
      illustration__record_valid_from
    ) AS _pit_hook__illustration,
    CONCAT('illustration|adventure_works|', illustration__illustration_id) AS _hook__illustration,
    *
  FROM validity
)
SELECT
  _pit_hook__illustration::BLOB,
  _hook__illustration::BLOB,
  illustration__illustration_id::VARCHAR,
  illustration__diagram::VARCHAR,
  illustration__modified_date::VARCHAR,
  illustration__record_loaded_at::TIMESTAMP,
  illustration__record_version::INT,
  illustration__record_valid_from::TIMESTAMP,
  illustration__record_valid_to::TIMESTAMP,
  illustration__is_current_record::BOOLEAN,
  illustration__record_updated_at::TIMESTAMP
FROM hooks
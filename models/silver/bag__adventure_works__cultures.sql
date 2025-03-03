MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    culture_id AS culture__culture_id,
    modified_date AS culture__modified_date,
    name AS culture__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS culture__record_loaded_at
  FROM bronze.raw__adventure_works__cultures
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY culture__culture_id ORDER BY culture__record_loaded_at) AS culture__record_version,
    CASE
      WHEN culture__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE culture__record_loaded_at
    END AS culture__record_valid_from,
    COALESCE(
      LEAD(culture__record_loaded_at) OVER (PARTITION BY culture__culture_id ORDER BY culture__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS culture__record_valid_to,
    culture__record_valid_to = @max_ts::TIMESTAMP AS culture__is_current_record,
    CASE
      WHEN culture__is_current_record
      THEN culture__record_loaded_at
      ELSE culture__record_valid_to
    END AS culture__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'culture|adventure_works|',
      culture__culture_id,
      '~epoch|valid_from|',
      culture__record_valid_from
    ) AS _pit_hook__culture,
    CONCAT('culture|adventure_works|', culture__culture_id) AS _hook__culture,
    *
  FROM validity
)
SELECT
  _pit_hook__culture::BLOB,
  _hook__culture::BLOB,
  culture__culture_id::VARCHAR,
  culture__modified_date::VARCHAR,
  culture__name::VARCHAR,
  culture__record_loaded_at::TIMESTAMP,
  culture__record_version::INT,
  culture__record_valid_from::TIMESTAMP,
  culture__record_valid_to::TIMESTAMP,
  culture__is_current_record::BOOLEAN,
  culture__record_updated_at::TIMESTAMP
FROM hooks
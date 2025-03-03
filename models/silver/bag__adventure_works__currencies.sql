MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    currency_code AS currency_code__currency_code,
    modified_date AS currency_code__modified_date,
    name AS currency_code__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency_code__record_loaded_at
  FROM bronze.raw__adventure_works__currencies
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_code__currency_code ORDER BY currency_code__record_loaded_at) AS currency_code__record_version,
    CASE
      WHEN currency_code__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE currency_code__record_loaded_at
    END AS currency_code__record_valid_from,
    COALESCE(
      LEAD(currency_code__record_loaded_at) OVER (PARTITION BY currency_code__currency_code ORDER BY currency_code__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS currency_code__record_valid_to,
    currency_code__record_valid_to = @max_ts::TIMESTAMP AS currency_code__is_current_record,
    CASE
      WHEN currency_code__is_current_record
      THEN currency_code__record_loaded_at
      ELSE currency_code__record_valid_to
    END AS currency_code__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency_code|adventure_works|',
      currency_code__currency_code,
      '~epoch|valid_from|',
      currency_code__record_valid_from
    ) AS _pit_hook__currency_code,
    CONCAT('currency_code|adventure_works|', currency_code__currency_code) AS _hook__currency_code,
    *
  FROM validity
)
SELECT
  _pit_hook__currency_code::BLOB,
  _hook__currency_code::BLOB,
  currency_code__currency_code::VARCHAR,
  currency_code__modified_date::VARCHAR,
  currency_code__name::VARCHAR,
  currency_code__record_loaded_at::TIMESTAMP,
  currency_code__record_version::INT,
  currency_code__record_valid_from::TIMESTAMP,
  currency_code__record_valid_to::TIMESTAMP,
  currency_code__is_current_record::BOOLEAN,
  currency_code__record_updated_at::TIMESTAMP
FROM hooks
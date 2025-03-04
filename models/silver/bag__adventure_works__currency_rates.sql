MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column currency_rat__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    currency_rate_id AS currency_rat__currency_rate_id,
    average_rate AS currency_rat__average_rate,
    currency_rate_date AS currency_rat__currency_rate_date,
    end_of_day_rate AS currency_rat__end_of_day_rate,
    from_currency_code AS currency_rat__from_currency_code,
    modified_date AS currency_rat__modified_date,
    to_currency_code AS currency_rat__to_currency_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency_rat__record_loaded_at
  FROM bronze.raw__adventure_works__currency_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_rat__currency_rate_id ORDER BY currency_rat__record_loaded_at) AS currency_rat__record_version,
    CASE
      WHEN currency_rat__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE currency_rat__record_loaded_at
    END AS currency_rat__record_valid_from,
    COALESCE(
      LEAD(currency_rat__record_loaded_at) OVER (PARTITION BY currency_rat__currency_rate_id ORDER BY currency_rat__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS currency_rat__record_valid_to,
    currency_rat__record_valid_to = @max_ts::TIMESTAMP AS currency_rat__is_current_record,
    CASE
      WHEN currency_rat__is_current_record
      THEN currency_rat__record_loaded_at
      ELSE currency_rat__record_valid_to
    END AS currency_rat__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency_rate|adventure_works|',
      currency_rat__currency_rate_id,
      '~epoch|valid_from|',
      currency_rat__record_valid_from
    ) AS _pit_hook__currency_rate,
    CONCAT('currency_rate|adventure_works|', currency_rat__currency_rate_id) AS _hook__currency_rate,
    *
  FROM validity
)
SELECT
  _pit_hook__currency_rate::BLOB,
  _hook__currency_rate::BLOB,
  currency_rat__currency_rate_id::BIGINT,
  currency_rat__average_rate::DOUBLE,
  currency_rat__currency_rate_date::VARCHAR,
  currency_rat__end_of_day_rate::DOUBLE,
  currency_rat__from_currency_code::VARCHAR,
  currency_rat__modified_date::VARCHAR,
  currency_rat__to_currency_code::VARCHAR,
  currency_rat__record_loaded_at::TIMESTAMP,
  currency_rat__record_updated_at::TIMESTAMP,
  currency_rat__record_valid_from::TIMESTAMP,
  currency_rat__record_valid_to::TIMESTAMP,
  currency_rat__record_version::INT,
  currency_rat__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND currency_rat__record_updated_at BETWEEN @start_ts AND @end_ts
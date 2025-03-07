MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__currency,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__currency, _hook__currency)
);

WITH staging AS (
  SELECT
    currency_code AS currency__currency_code,
    name AS currency__name,
    modified_date AS currency__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency__record_loaded_at
  FROM bronze.raw__adventure_works__currencies
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency__currency_code ORDER BY currency__record_loaded_at) AS currency__record_version,
    CASE
      WHEN currency__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE currency__record_loaded_at
    END AS currency__record_valid_from,
    COALESCE(
      LEAD(currency__record_loaded_at) OVER (PARTITION BY currency__currency_code ORDER BY currency__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS currency__record_valid_to,
    currency__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS currency__is_current_record,
    CASE
      WHEN currency__is_current_record
      THEN currency__record_loaded_at
      ELSE currency__record_valid_to
    END AS currency__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'currency__adventure_works|',
      currency__currency_code,
      '~epoch|valid_from|',
      currency__record_valid_from
    )::BLOB AS _pit_hook__currency,
    CONCAT('currency__adventure_works|', currency__currency_code) AS _hook__currency,
    *
  FROM validity
)
SELECT
  _pit_hook__currency::BLOB,
  _hook__currency::BLOB,
  currency__currency_code::TEXT,
  currency__name::TEXT,
  currency__modified_date::DATE,
  currency__record_loaded_at::TIMESTAMP,
  currency__record_updated_at::TIMESTAMP,
  currency__record_version::TEXT,
  currency__record_valid_from::TIMESTAMP,
  currency__record_valid_to::TIMESTAMP,
  currency__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND currency__record_updated_at BETWEEN @start_ts AND @end_ts
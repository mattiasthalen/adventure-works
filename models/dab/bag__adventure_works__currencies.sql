MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__currency
  ),
  tags hook,
  grain (_pit_hook__currency, _hook__currency),
  description 'Hook viewpoint of currencies data: Lookup table containing standard ISO currencies.',
  column_descriptions (
    currency__currency_code = 'The ISO code for the Currency.',
    currency__name = 'Currency name.',
    currency__record_loaded_at = 'Timestamp when this record was loaded into the system',
    currency__record_updated_at = 'Timestamp when this record was last updated',
    currency__record_version = 'Version number for this record',
    currency__record_valid_from = 'Timestamp from which this record version is valid',
    currency__record_valid_to = 'Timestamp until which this record version is valid',
    currency__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__currency = 'Reference hook to currency',
    _pit_hook__currency = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    currency_code AS currency__currency_code,
    name AS currency__name,
    modified_date AS currency__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency__record_loaded_at
  FROM das.raw__adventure_works__currencies
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
    CONCAT('currency__adventure_works|', currency__currency_code) AS _hook__currency,
    CONCAT_WS('~',
      _hook__currency,
      'epoch__valid_from|'||currency__record_valid_from
    ) AS _pit_hook__currency,
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
  currency__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND currency__record_updated_at BETWEEN @start_ts AND @end_ts
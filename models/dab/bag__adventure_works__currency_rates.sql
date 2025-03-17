MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__currency_rate
  ),
  tags hook,
  grain (_pit_hook__currency_rate, _hook__currency_rate),
  description 'Hook viewpoint of currency_rates data: Currency exchange rates.',
  references (_hook__currency__from, _hook__currency__to),
  column_descriptions (
    currency_rate__currency_rate_id = 'Primary key for CurrencyRate records.',
    currency_rate__currency_rate_date = 'Date and time the exchange rate was obtained.',
    currency_rate__from_currency_code = 'Exchange rate was converted from this currency code.',
    currency_rate__to_currency_code = 'Exchange rate was converted to this currency code.',
    currency_rate__average_rate = 'Average exchange rate for the day.',
    currency_rate__end_of_day_rate = 'Final exchange rate for the day.',
    currency_rate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    currency_rate__record_updated_at = 'Timestamp when this record was last updated',
    currency_rate__record_version = 'Version number for this record',
    currency_rate__record_valid_from = 'Timestamp from which this record version is valid',
    currency_rate__record_valid_to = 'Timestamp until which this record version is valid',
    currency_rate__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__currency__from = 'Reference hook to from currency',
    _hook__currency__to = 'Reference hook to to currency',
    _hook__currency_rate = 'Reference hook to currency_rate',
    _pit_hook__currency_rate = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    currency_rate_id AS currency_rate__currency_rate_id,
    currency_rate_date AS currency_rate__currency_rate_date,
    from_currency_code AS currency_rate__from_currency_code,
    to_currency_code AS currency_rate__to_currency_code,
    average_rate AS currency_rate__average_rate,
    end_of_day_rate AS currency_rate__end_of_day_rate,
    modified_date AS currency_rate__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS currency_rate__record_loaded_at
  FROM das.raw__adventure_works__currency_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency_rate__currency_rate_id ORDER BY currency_rate__record_loaded_at) AS currency_rate__record_version,
    CASE
      WHEN currency_rate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE currency_rate__record_loaded_at
    END AS currency_rate__record_valid_from,
    COALESCE(
      LEAD(currency_rate__record_loaded_at) OVER (PARTITION BY currency_rate__currency_rate_id ORDER BY currency_rate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS currency_rate__record_valid_to,
    currency_rate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS currency_rate__is_current_record,
    CASE
      WHEN currency_rate__is_current_record
      THEN currency_rate__record_loaded_at
      ELSE currency_rate__record_valid_to
    END AS currency_rate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('currency__adventure_works|', currency_rate__from_currency_code) AS _hook__currency__from,
    CONCAT('currency__adventure_works|', currency_rate__to_currency_code) AS _hook__currency__to,
    CONCAT('currency__adventure_works|', currency_rate__currency_rate_id) AS _hook__currency_rate,
    CONCAT_WS('~',
      _hook__currency_rate,
      'epoch__valid_from|'||currency_rate__record_valid_from
    ) AS _pit_hook__currency_rate,
    *
  FROM validity
)
SELECT
  _pit_hook__currency_rate::BLOB,
  _hook__currency__from::BLOB,
  _hook__currency__to::BLOB,
  _hook__currency_rate::BLOB,
  currency_rate__currency_rate_id::BIGINT,
  currency_rate__currency_rate_date::DATE,
  currency_rate__from_currency_code::TEXT,
  currency_rate__to_currency_code::TEXT,
  currency_rate__average_rate::DOUBLE,
  currency_rate__end_of_day_rate::DOUBLE,
  currency_rate__modified_date::DATE,
  currency_rate__record_loaded_at::TIMESTAMP,
  currency_rate__record_updated_at::TIMESTAMP,
  currency_rate__record_version::TEXT,
  currency_rate__record_valid_from::TIMESTAMP,
  currency_rate__record_valid_to::TIMESTAMP,
  currency_rate__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND currency_rate__record_updated_at BETWEEN @start_ts AND @end_ts
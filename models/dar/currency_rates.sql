MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__currency_rate),
  description 'Business viewpoint of currency_rates data: Currency exchange rates.',
  column_descriptions (
    currency_rate__currency_rate_id = 'Primary key for CurrencyRate records.',
    currency_rate__currency_rate_date = 'Date and time the exchange rate was obtained.',
    currency_rate__from_currency_code = 'Exchange rate was converted from this currency code.',
    currency_rate__to_currency_code = 'Exchange rate was converted to this currency code.',
    currency_rate__average_rate = 'Average exchange rate for the day.',
    currency_rate__end_of_day_rate = 'Final exchange rate for the day.',
    currency_rate__modified_date = 'Date when this record was last modified',
    currency_rate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    currency_rate__record_updated_at = 'Timestamp when this record was last updated',
    currency_rate__record_version = 'Version number for this record',
    currency_rate__record_valid_from = 'Timestamp from which this record version is valid',
    currency_rate__record_valid_to = 'Timestamp until which this record version is valid',
    currency_rate__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__currency_rate,
    currency_rate__currency_rate_id,
    currency_rate__currency_rate_date,
    currency_rate__from_currency_code,
    currency_rate__to_currency_code,
    currency_rate__average_rate,
    currency_rate__end_of_day_rate,
    currency_rate__modified_date,
    currency_rate__record_loaded_at,
    currency_rate__record_updated_at,
    currency_rate__record_version,
    currency_rate__record_valid_from,
    currency_rate__record_valid_to,
    currency_rate__is_current_record
  FROM dab.bag__adventure_works__currency_rates
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__currency_rate,
    NULL AS currency_rate__currency_rate_id,
    NULL AS currency_rate__currency_rate_date,
    'N/A' AS currency_rate__from_currency_code,
    'N/A' AS currency_rate__to_currency_code,
    NULL AS currency_rate__average_rate,
    NULL AS currency_rate__end_of_day_rate,
    NULL AS currency_rate__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS currency_rate__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS currency_rate__record_updated_at,
    0 AS currency_rate__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS currency_rate__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS currency_rate__record_valid_to,
    TRUE AS currency_rate__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__currency_rate::BLOB,
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
  currency_rate__is_current_record::BOOLEAN
FROM cte__final
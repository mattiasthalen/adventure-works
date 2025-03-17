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

SELECT
  *
  EXCLUDE (_hook__currency__from, _hook__currency__to, _hook__currency_rate)
FROM dab.bag__adventure_works__currency_rates
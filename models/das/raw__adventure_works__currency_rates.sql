MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of currency_rates data: Currency exchange rates.',
  column_descriptions (
    currency_rate_id = 'Primary key for CurrencyRate records.',
    currency_rate_date = 'Date and time the exchange rate was obtained.',
    from_currency_code = 'Exchange rate was converted from this currency code.',
    to_currency_code = 'Exchange rate was converted to this currency code.',
    average_rate = 'Average exchange rate for the day.',
    end_of_day_rate = 'Final exchange rate for the day.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    currency_rate_id::BIGINT,
    currency_rate_date::DATE,
    from_currency_code::TEXT,
    to_currency_code::TEXT,
    average_rate::DOUBLE,
    end_of_day_rate::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__currency_rates"
)
;
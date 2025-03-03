MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  currency_rate_id,
  average_rate,
  currency_rate_date,
  end_of_day_rate,
  from_currency_code,
  modified_date,
  to_currency_code,
  _dlt_load_id
  FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__currency_rates"
  )
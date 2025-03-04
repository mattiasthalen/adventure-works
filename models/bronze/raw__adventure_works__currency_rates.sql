MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  currency_rate_id::BIGINT,
  average_rate::DOUBLE,
  currency_rate_date::VARCHAR,
  end_of_day_rate::DOUBLE,
  from_currency_code::VARCHAR,
  modified_date::VARCHAR,
  to_currency_code::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__currency_rates"
);

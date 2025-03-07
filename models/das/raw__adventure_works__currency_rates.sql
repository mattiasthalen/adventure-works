MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    currency_rate_id::BIGINT,
    currency_rate_date::TEXT,
    from_currency_code::TEXT,
    to_currency_code::TEXT,
    average_rate::DOUBLE,
    end_of_day_rate::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__currency_rates"
)
;
MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  sales_tax_rate_id::BIGINT,
  state_province_id::BIGINT,
  modified_date::VARCHAR,
  name::VARCHAR,
  rowguid::VARCHAR,
  tax_rate::DOUBLE,
  tax_type::BIGINT,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_tax_rates"
);

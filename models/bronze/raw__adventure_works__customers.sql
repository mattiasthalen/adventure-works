MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  customer_id::BIGINT,
  person_id::BIGINT,
  store_id::BIGINT,
  territory_id::BIGINT,
  account_number::VARCHAR,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__customers"
);

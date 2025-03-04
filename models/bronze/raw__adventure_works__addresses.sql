MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  address_id::BIGINT,
  state_province_id::BIGINT,
  address_line1::VARCHAR,
  address_line2::VARCHAR,
  city::VARCHAR,
  modified_date::VARCHAR,
  postal_code::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__addresses"
);

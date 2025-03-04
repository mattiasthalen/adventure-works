MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  email_address_id::BIGINT,
  email::VARCHAR,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__email_addresses"
);

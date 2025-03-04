MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  account_number::VARCHAR,
  active_flag::BOOLEAN,
  credit_rating::BIGINT,
  modified_date::VARCHAR,
  name::VARCHAR,
  preferred_vendor_status::BOOLEAN,
  purchasing_web_service_url::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__vendors"
);

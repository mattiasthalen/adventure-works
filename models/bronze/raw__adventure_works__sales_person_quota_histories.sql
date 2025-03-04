MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  modified_date::VARCHAR,
  quota_date::VARCHAR,
  rowguid::VARCHAR,
  sales_quota::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_person_quota_histories"
);

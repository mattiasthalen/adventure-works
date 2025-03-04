MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  contact_type_id::BIGINT,
  person_id::BIGINT,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__business_entity_contacts"
);

MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  demographics::VARCHAR,
  email_promotion::BIGINT,
  first_name::VARCHAR,
  last_name::VARCHAR,
  middle_name::VARCHAR,
  modified_date::VARCHAR,
  name_style::BOOLEAN,
  person_type::VARCHAR,
  rowguid::VARCHAR,
  suffix::VARCHAR,
  title::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__persons"
);

MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  --additional_contact_info,
  demographics,
  email_promotion,
  first_name,
  last_name,
  middle_name,
  modified_date,
  name_style,
  person_type,
  rowguid,
  suffix,
  title,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__persons"
)

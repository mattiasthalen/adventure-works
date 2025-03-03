MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  contact_type_id,
  modified_date,
  person_id,
  rowguid,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__business_entity_contacts"
)

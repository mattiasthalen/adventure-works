MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  modified_date,
  phone_number,
  phone_number_type_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__person_phones"
)

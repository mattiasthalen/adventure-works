MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  demographics,
  modified_date,
  name,
  rowguid,
  sales_person_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__stores"
)

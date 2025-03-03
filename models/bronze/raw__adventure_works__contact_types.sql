MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  contact_type_id,
  modified_date,
  name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__contact_types"
)

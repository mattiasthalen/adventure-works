MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  modified_date,
  quota_date,
  rowguid,
  sales_quota,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_person_quota_histories"
)

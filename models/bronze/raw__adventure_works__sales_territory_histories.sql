MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  end_date,
  modified_date,
  rowguid,
  start_date,
  territory_id,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territory_histories"
)

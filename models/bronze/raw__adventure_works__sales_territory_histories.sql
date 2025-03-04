MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  territory_id::BIGINT,
  end_date::VARCHAR,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  start_date::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territory_histories"
);

MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  territory_id::BIGINT,
  bonus::DOUBLE,
  commission_pct::DOUBLE,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  sales_last_year::DOUBLE,
  sales_quota::DOUBLE,
  sales_ytd::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_persons"
);

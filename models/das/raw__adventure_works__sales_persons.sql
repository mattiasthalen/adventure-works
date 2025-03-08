MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    bonus::DOUBLE,
    commission_pct::DOUBLE,
    sales_ytd::DOUBLE,
    sales_last_year::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    territory_id::BIGINT,
    sales_quota::DOUBLE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_persons"
)
;
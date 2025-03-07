MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    territory_id::BIGINT,
    start_date::TEXT,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__sales_territory_histories"
)
;
MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    location_id::BIGINT,
    name::TEXT,
    cost_rate::DOUBLE,
    availability::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__locations"
)
;
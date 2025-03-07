MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    unit_measure_code::TEXT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__unit_measures"
)
;
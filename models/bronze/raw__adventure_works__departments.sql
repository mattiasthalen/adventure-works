MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    department_id::BIGINT,
    name::TEXT,
    group_name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__departments"
)
;
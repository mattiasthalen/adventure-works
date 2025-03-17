MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of departments data: Lookup table containing the departments within the Adventure Works Cycles company.',
  column_descriptions (
    department_id = 'Primary key for Department records.',
    name = 'Name of the department.',
    group_name = 'Name of the group to which the department belongs.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    department_id::BIGINT,
    name::TEXT,
    group_name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__departments"
)
;
MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    business_entity_id::BIGINT,
    department_id::BIGINT,
    shift_id::BIGINT,
    start_date::DATE,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::DATE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__employee_department_histories"
)
;
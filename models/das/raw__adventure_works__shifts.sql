MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    shift_id::BIGINT,
    name::TEXT,
    start_time::TEXT,
    end_time::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__shifts"
)
;
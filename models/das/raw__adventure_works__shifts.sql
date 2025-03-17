MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of shifts data: Work shift lookup table.',
  column_descriptions (
    shift_id = 'Primary key for Shift records.',
    name = 'Shift description.',
    start_time = 'Shift start time. ISO duration.',
    end_time = 'Shift end time. ISO duration.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
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
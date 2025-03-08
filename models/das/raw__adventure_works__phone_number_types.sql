MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    phone_number_type_id::BIGINT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__phone_number_types"
)
;
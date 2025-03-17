MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of phone_number_types data: Type of phone number of a person.',
  column_descriptions (
    phone_number_type_id = 'Primary key for telephone number type records.',
    name = 'Name of the telephone number type.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
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
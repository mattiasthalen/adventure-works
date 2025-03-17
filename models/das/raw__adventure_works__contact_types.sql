MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of contact_types data: Lookup table containing the types of business entity contacts.',
  column_descriptions (
    contact_type_id = 'Primary key for ContactType records.',
    name = 'Contact type description.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    contact_type_id::BIGINT,
    name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__contact_types"
)
;
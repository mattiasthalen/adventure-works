MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of addresses data: Street address information for customers, employees, and vendors.',
  column_descriptions (
    address_id = 'Primary key for Address records.',
    address_line1 = 'First street address line.',
    city = 'Name of the city.',
    state_province_id = 'Unique identification number for the state or province. Foreign key to StateProvince table.',
    postal_code = 'Postal code for the street address.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    address_line2 = 'Second street address line.'
  )
);

SELECT
    address_id::BIGINT,
    address_line1::TEXT,
    city::TEXT,
    state_province_id::BIGINT,
    postal_code::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    address_line2::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__addresses"
)
;
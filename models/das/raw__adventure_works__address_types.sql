MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of address_types data: Types of addresses stored in the Address table.',
  column_descriptions (
    address_type_id = 'Primary key for AddressType records.',
    name = 'Address type description. For example, Billing, Home, or Shipping.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    address_type_id::BIGINT,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__address_types"
)
;
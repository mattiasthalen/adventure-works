MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of business_entity_addresses data: Cross-reference table mapping customers, vendors, and employees to their addresses.',
  column_descriptions (
    business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    address_id = 'Primary key. Foreign key to Address.AddressID.',
    address_type_id = 'Primary key. Foreign key to AddressType.AddressTypeID.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    address_id::BIGINT,
    address_type_id::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__business_entity_addresses"
)
;
MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of business_entity_contacts data: Cross-reference table mapping stores, vendors, and employees to people.',
  column_descriptions (
    business_entity_id = 'Primary key. Foreign key to BusinessEntity.BusinessEntityID.',
    person_id = 'Primary key. Foreign key to Person.BusinessEntityID.',
    contact_type_id = 'Primary key. Foreign key to ContactType.ContactTypeID.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    person_id::BIGINT,
    contact_type_id::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__business_entity_contacts"
)
;
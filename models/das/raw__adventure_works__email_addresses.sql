MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of email_addresses data: Where to send a person email.',
  column_descriptions (
    business_entity_id = 'Primary key. Person associated with this email address. Foreign key to Person.BusinessEntityID.',
    email_address_id = 'Primary key. ID of this email address.',
    email = 'E-mail address for the person.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    email_address_id::BIGINT,
    email::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__email_addresses"
)
;
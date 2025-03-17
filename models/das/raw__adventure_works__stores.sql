MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of stores data: Customers (resellers) of Adventure Works products.',
  column_descriptions (
    business_entity_id = 'Primary key. Foreign key to Customer.BusinessEntityID.',
    name = 'Name of the store.',
    sales_person_id = 'ID of the sales person assigned to the customer. Foreign key to SalesPerson.BusinessEntityID.',
    demographics = 'Demographic information about the store such as the number of employees, annual sales and store type.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    business_entity_id::BIGINT,
    name::TEXT,
    sales_person_id::BIGINT,
    demographics::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__stores"
)
;
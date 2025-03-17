MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of customers data: Current customer information. Also see the Person and Store tables.',
  column_descriptions (
    customer_id = 'Primary key.',
    store_id = 'Foreign key to Store.BusinessEntityID.',
    territory_id = 'ID of the territory in which the customer is located. Foreign key to SalesTerritory.SalesTerritoryID.',
    account_number = 'Unique number identifying the customer assigned by the accounting system.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    person_id = 'Foreign key to Person.BusinessEntityID.'
  )
);

SELECT
    customer_id::BIGINT,
    store_id::BIGINT,
    territory_id::BIGINT,
    account_number::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    person_id::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__customers"
)
;
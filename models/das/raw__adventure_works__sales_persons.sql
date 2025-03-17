MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_persons data: Sales representative current information.',
  column_descriptions (
    business_entity_id = 'Primary key for SalesPerson records. Foreign key to Employee.BusinessEntityID.',
    bonus = 'Bonus due if quota is met.',
    commission_pct = 'Commission percent received per sale.',
    sales_ytd = 'Sales total year to date.',
    sales_last_year = 'Sales total of previous year.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    territory_id = 'Territory currently assigned to. Foreign key to SalesTerritory.SalesTerritoryID.',
    sales_quota = 'Projected yearly sales.'
  )
);

SELECT
    business_entity_id::BIGINT,
    bonus::DOUBLE,
    commission_pct::DOUBLE,
    sales_ytd::DOUBLE,
    sales_last_year::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    territory_id::BIGINT,
    sales_quota::DOUBLE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_persons"
)
;
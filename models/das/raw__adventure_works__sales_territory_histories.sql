MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_territory_histories data: Sales representative transfers to other sales territories.',
  column_descriptions (
    business_entity_id = 'Primary key. The sales rep. Foreign key to SalesPerson.BusinessEntityID.',
    territory_id = 'Primary key. Territory identification number. Foreign key to SalesTerritory.SalesTerritoryID.',
    start_date = 'Primary key. Date the sales representative started work in the territory.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    end_date = 'Date the sales representative left work in the territory.'
  )
);

SELECT
    business_entity_id::BIGINT,
    territory_id::BIGINT,
    start_date::DATE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    end_date::DATE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_territory_histories"
)
;
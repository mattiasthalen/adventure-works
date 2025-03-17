MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__sales),
  description 'Business viewpoint of sales_persons data: Sales representative current information.',
  column_descriptions (
    sales_person__business_entity_id = 'Primary key for SalesPerson records. Foreign key to Employee.BusinessEntityID.',
    sales_person__bonus = 'Bonus due if quota is met.',
    sales_person__commission_pct = 'Commission percent received per sale.',
    sales_person__sales_ytd = 'Sales total year to date.',
    sales_person__sales_last_year = 'Sales total of previous year.',
    sales_person__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_person__territory_id = 'Territory currently assigned to. Foreign key to SalesTerritory.SalesTerritoryID.',
    sales_person__sales_quota = 'Projected yearly sales.',
    sales_person__modified_date = 'Date when this record was last modified',
    sales_person__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_person__record_updated_at = 'Timestamp when this record was last updated',
    sales_person__record_version = 'Version number for this record',
    sales_person__record_valid_from = 'Timestamp from which this record version is valid',
    sales_person__record_valid_to = 'Timestamp until which this record version is valid',
    sales_person__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__person__sales, _hook__territory__sales)
FROM dab.bag__adventure_works__sales_persons
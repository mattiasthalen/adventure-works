MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__person__sales),
  description 'Business viewpoint of sales_territory_histories data: Sales representative transfers to other sales territories.',
  column_descriptions (
    sales_territory_history__business_entity_id = 'Primary key. The sales rep. Foreign key to SalesPerson.BusinessEntityID.',
    sales_territory_history__territory_id = 'Primary key. Territory identification number. Foreign key to SalesTerritory.SalesTerritoryID.',
    sales_territory_history__start_date = 'Primary key. Date the sales representative started work in the territory.',
    sales_territory_history__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_territory_history__end_date = 'Date the sales representative left work in the territory.',
    sales_territory_history__modified_date = 'Date when this record was last modified',
    sales_territory_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_territory_history__record_updated_at = 'Timestamp when this record was last updated',
    sales_territory_history__record_version = 'Version number for this record',
    sales_territory_history__record_valid_from = 'Timestamp from which this record version is valid',
    sales_territory_history__record_valid_to = 'Timestamp until which this record version is valid',
    sales_territory_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__person__sales, _hook__territory__sales)
FROM dab.bag__adventure_works__sales_territory_histories
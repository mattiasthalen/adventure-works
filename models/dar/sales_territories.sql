MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__territory__sales),
  description 'Business viewpoint of sales_territories data: Sales territory lookup table.',
  column_descriptions (
    sales_territory__territory_id = 'Primary key for SalesTerritory records.',
    sales_territory__name = 'Sales territory description.',
    sales_territory__country_region_code = 'ISO standard country or region code. Foreign key to CountryRegion.CountryRegionCode.',
    sales_territory__group = 'Geographic area to which the sales territory belongs.',
    sales_territory__sales_ytd = 'Sales in the territory year to date.',
    sales_territory__sales_last_year = 'Sales in the territory the previous year.',
    sales_territory__cost_ytd = 'Business costs in the territory year to date.',
    sales_territory__cost_last_year = 'Business costs in the territory the previous year.',
    sales_territory__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_territory__modified_date = 'Date when this record was last modified',
    sales_territory__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_territory__record_updated_at = 'Timestamp when this record was last updated',
    sales_territory__record_version = 'Version number for this record',
    sales_territory__record_valid_from = 'Timestamp from which this record version is valid',
    sales_territory__record_valid_to = 'Timestamp until which this record version is valid',
    sales_territory__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__territory__sales, _hook__reference__country_region)
FROM dab.bag__adventure_works__sales_territories
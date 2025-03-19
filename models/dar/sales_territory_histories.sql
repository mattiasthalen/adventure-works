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

WITH cte__source AS (
  SELECT
    _pit_hook__person__sales,
    sales_territory_history__business_entity_id,
    sales_territory_history__territory_id,
    sales_territory_history__start_date,
    sales_territory_history__rowguid,
    sales_territory_history__end_date,
    sales_territory_history__modified_date,
    sales_territory_history__record_loaded_at,
    sales_territory_history__record_updated_at,
    sales_territory_history__record_version,
    sales_territory_history__record_valid_from,
    sales_territory_history__record_valid_to,
    sales_territory_history__is_current_record
  FROM dab.bag__adventure_works__sales_territory_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__sales,
    NULL AS sales_territory_history__business_entity_id,
    NULL AS sales_territory_history__territory_id,
    NULL AS sales_territory_history__start_date,
    'N/A' AS sales_territory_history__rowguid,
    NULL AS sales_territory_history__end_date,
    NULL AS sales_territory_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory_history__record_updated_at,
    0 AS sales_territory_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_territory_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_territory_history__record_valid_to,
    TRUE AS sales_territory_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__sales::BLOB,
  sales_territory_history__business_entity_id::BIGINT,
  sales_territory_history__territory_id::BIGINT,
  sales_territory_history__start_date::DATE,
  sales_territory_history__rowguid::TEXT,
  sales_territory_history__end_date::DATE,
  sales_territory_history__modified_date::DATE,
  sales_territory_history__record_loaded_at::TIMESTAMP,
  sales_territory_history__record_updated_at::TIMESTAMP,
  sales_territory_history__record_version::TEXT,
  sales_territory_history__record_valid_from::TIMESTAMP,
  sales_territory_history__record_valid_to::TIMESTAMP,
  sales_territory_history__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.sales_territory_histories TO './export/dar/sales_territory_histories.parquet' (FORMAT parquet, COMPRESSION zstd)
);
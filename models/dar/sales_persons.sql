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

WITH cte__source AS (
  SELECT
    _pit_hook__person__sales,
    sales_person__business_entity_id,
    sales_person__bonus,
    sales_person__commission_pct,
    sales_person__sales_ytd,
    sales_person__sales_last_year,
    sales_person__rowguid,
    sales_person__territory_id,
    sales_person__sales_quota,
    sales_person__modified_date,
    sales_person__record_loaded_at,
    sales_person__record_updated_at,
    sales_person__record_version,
    sales_person__record_valid_from,
    sales_person__record_valid_to,
    sales_person__is_current_record
  FROM dab.bag__adventure_works__sales_persons
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__person__sales,
    NULL AS sales_person__business_entity_id,
    NULL AS sales_person__bonus,
    NULL AS sales_person__commission_pct,
    NULL AS sales_person__sales_ytd,
    NULL AS sales_person__sales_last_year,
    'N/A' AS sales_person__rowguid,
    NULL AS sales_person__territory_id,
    NULL AS sales_person__sales_quota,
    NULL AS sales_person__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person__record_updated_at,
    0 AS sales_person__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_person__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_person__record_valid_to,
    TRUE AS sales_person__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__person__sales::BLOB,
  sales_person__business_entity_id::BIGINT,
  sales_person__bonus::DOUBLE,
  sales_person__commission_pct::DOUBLE,
  sales_person__sales_ytd::DOUBLE,
  sales_person__sales_last_year::DOUBLE,
  sales_person__rowguid::TEXT,
  sales_person__territory_id::BIGINT,
  sales_person__sales_quota::DOUBLE,
  sales_person__modified_date::DATE,
  sales_person__record_loaded_at::TIMESTAMP,
  sales_person__record_updated_at::TIMESTAMP,
  sales_person__record_version::TEXT,
  sales_person__record_valid_from::TIMESTAMP,
  sales_person__record_valid_to::TIMESTAMP,
  sales_person__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.sales_persons TO './export/dar/sales_persons.parquet' (FORMAT parquet, COMPRESSION zstd)
);
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__sales
  ),
  tags hook,
  grain (_pit_hook__person__sales, _hook__person__sales),
  description 'Hook viewpoint of sales_territory_histories data: Sales representative transfers to other sales territories.',
  references (_hook__territory__sales),
  column_descriptions (
    sales_territory_history__business_entity_id = 'Primary key. The sales rep. Foreign key to SalesPerson.BusinessEntityID.',
    sales_territory_history__territory_id = 'Primary key. Territory identification number. Foreign key to SalesTerritory.SalesTerritoryID.',
    sales_territory_history__start_date = 'Primary key. Date the sales representative started work in the territory.',
    sales_territory_history__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_territory_history__end_date = 'Date the sales representative left work in the territory.',
    sales_territory_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_territory_history__record_updated_at = 'Timestamp when this record was last updated',
    sales_territory_history__record_version = 'Version number for this record',
    sales_territory_history__record_valid_from = 'Timestamp from which this record version is valid',
    sales_territory_history__record_valid_to = 'Timestamp until which this record version is valid',
    sales_territory_history__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__person__sales = 'Reference hook to sales person',
    _hook__territory__sales = 'Reference hook to sales territory',
    _pit_hook__person__sales = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    business_entity_id AS sales_territory_history__business_entity_id,
    territory_id AS sales_territory_history__territory_id,
    start_date AS sales_territory_history__start_date,
    rowguid AS sales_territory_history__rowguid,
    end_date AS sales_territory_history__end_date,
    modified_date AS sales_territory_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_territory_history__record_loaded_at
  FROM das.raw__adventure_works__sales_territory_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_territory_history__business_entity_id ORDER BY sales_territory_history__record_loaded_at) AS sales_territory_history__record_version,
    CASE
      WHEN sales_territory_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_territory_history__record_loaded_at
    END AS sales_territory_history__record_valid_from,
    COALESCE(
      LEAD(sales_territory_history__record_loaded_at) OVER (PARTITION BY sales_territory_history__business_entity_id ORDER BY sales_territory_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_territory_history__record_valid_to,
    sales_territory_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_territory_history__is_current_record,
    CASE
      WHEN sales_territory_history__is_current_record
      THEN sales_territory_history__record_loaded_at
      ELSE sales_territory_history__record_valid_to
    END AS sales_territory_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__sales__adventure_works|', sales_territory_history__business_entity_id) AS _hook__person__sales,
    CONCAT('territory__sales__adventure_works|', sales_territory_history__territory_id) AS _hook__territory__sales,
    CONCAT_WS('~',
      _hook__person__sales,
      'epoch__valid_from|'||sales_territory_history__record_valid_from
    ) AS _pit_hook__person__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__person__sales::BLOB,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
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
  sales_territory_history__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_territory_history__record_updated_at BETWEEN @start_ts AND @end_ts
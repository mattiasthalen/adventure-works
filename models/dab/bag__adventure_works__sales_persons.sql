MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__sales
  ),
  tags hook,
  grain (_pit_hook__person__sales, _hook__person__sales),
  references (_hook__territory__sales)
);

WITH staging AS (
  SELECT
    business_entity_id AS sales_person__business_entity_id,
    bonus AS sales_person__bonus,
    commission_pct AS sales_person__commission_pct,
    sales_ytd AS sales_person__sales_ytd,
    sales_last_year AS sales_person__sales_last_year,
    rowguid AS sales_person__rowguid,
    territory_id AS sales_person__territory_id,
    sales_quota AS sales_person__sales_quota,
    modified_date AS sales_person__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_person__record_loaded_at
  FROM das.raw__adventure_works__sales_persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_person__business_entity_id ORDER BY sales_person__record_loaded_at) AS sales_person__record_version,
    CASE
      WHEN sales_person__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_person__record_loaded_at
    END AS sales_person__record_valid_from,
    COALESCE(
      LEAD(sales_person__record_loaded_at) OVER (PARTITION BY sales_person__business_entity_id ORDER BY sales_person__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_person__record_valid_to,
    sales_person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_person__is_current_record,
    CASE
      WHEN sales_person__is_current_record
      THEN sales_person__record_loaded_at
      ELSE sales_person__record_valid_to
    END AS sales_person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('person__sales__adventure_works|', sales_person__business_entity_id) AS _hook__person__sales,
    CONCAT('territory__sales__adventure_works|', sales_person__territory_id) AS _hook__territory__sales,
    CONCAT_WS('~',
      _hook__person__sales,
      'epoch__valid_from|'||sales_person__record_valid_from
    ) AS _pit_hook__person__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__person__sales::BLOB,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
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
  sales_person__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_person__record_updated_at BETWEEN @start_ts AND @end_ts
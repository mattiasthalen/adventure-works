MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column store__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__store, _hook__store),
  references (_hook__person__sales)
);

WITH staging AS (
  SELECT
    business_entity_id AS store__business_entity_id,
    name AS store__name,
    sales_person_id AS store__sales_person_id,
    demographics AS store__demographics,
    rowguid AS store__rowguid,
    modified_date AS store__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS store__record_loaded_at
  FROM bronze.raw__adventure_works__stores
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY store__business_entity_id ORDER BY store__record_loaded_at) AS store__record_version,
    CASE
      WHEN store__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE store__record_loaded_at
    END AS store__record_valid_from,
    COALESCE(
      LEAD(store__record_loaded_at) OVER (PARTITION BY store__business_entity_id ORDER BY store__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS store__record_valid_to,
    store__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS store__is_current_record,
    CASE
      WHEN store__is_current_record
      THEN store__record_loaded_at
      ELSE store__record_valid_to
    END AS store__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'store__adventure_works|',
      store__business_entity_id,
      '~epoch__valid_from|',
      store__record_valid_from
    )::BLOB AS _pit_hook__store,
    CONCAT('store__adventure_works|', store__business_entity_id) AS _hook__store,
    CONCAT('person__sales__adventure_works|', store__sales_person_id) AS _hook__person__sales,
    *
  FROM validity
)
SELECT
  _pit_hook__store::BLOB,
  _hook__store::BLOB,
  _hook__person__sales::BLOB,
  store__business_entity_id::BIGINT,
  store__name::TEXT,
  store__sales_person_id::BIGINT,
  store__demographics::TEXT,
  store__rowguid::TEXT,
  store__modified_date::DATE,
  store__record_loaded_at::TIMESTAMP,
  store__record_updated_at::TIMESTAMP,
  store__record_version::TEXT,
  store__record_valid_from::TIMESTAMP,
  store__record_valid_to::TIMESTAMP,
  store__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND store__record_updated_at BETWEEN @start_ts AND @end_ts
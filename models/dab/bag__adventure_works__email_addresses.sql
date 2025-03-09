MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__individual,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__individual, _hook__person__individual),
  references (_hook__email_address)
);

WITH staging AS (
  SELECT
    business_entity_id AS email_address__business_entity_id,
    email_address_id AS email_address__email_address_id,
    email AS email_address__email,
    rowguid AS email_address__rowguid,
    modified_date AS email_address__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS email_address__record_loaded_at
  FROM das.raw__adventure_works__email_addresses
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY email_address__business_entity_id ORDER BY email_address__record_loaded_at) AS email_address__record_version,
    CASE
      WHEN email_address__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE email_address__record_loaded_at
    END AS email_address__record_valid_from,
    COALESCE(
      LEAD(email_address__record_loaded_at) OVER (PARTITION BY email_address__business_entity_id ORDER BY email_address__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS email_address__record_valid_to,
    email_address__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS email_address__is_current_record,
    CASE
      WHEN email_address__is_current_record
      THEN email_address__record_loaded_at
      ELSE email_address__record_valid_to
    END AS email_address__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'person__individual__adventure_works|',
      email_address__business_entity_id,
      '~epoch__valid_from|',
      email_address__record_valid_from
    )::BLOB AS _pit_hook__person__individual,
    CONCAT('person__individual__adventure_works|', email_address__business_entity_id) AS _hook__person__individual,
    CONCAT('reference__adventure_works|', email_address__email_address_id) AS _hook__email_address,
    *
  FROM validity
)
SELECT
  _pit_hook__person__individual::BLOB,
  _hook__person__individual::BLOB,
  _hook__email_address::BLOB,
  email_address__business_entity_id::BIGINT,
  email_address__email_address_id::BIGINT,
  email_address__email::TEXT,
  email_address__rowguid::TEXT,
  email_address__modified_date::DATE,
  email_address__record_loaded_at::TIMESTAMP,
  email_address__record_updated_at::TIMESTAMP,
  email_address__record_version::TEXT,
  email_address__record_valid_from::TIMESTAMP,
  email_address__record_valid_to::TIMESTAMP,
  email_address__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND email_address__record_updated_at BETWEEN @start_ts AND @end_ts
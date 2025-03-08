MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__ship_method,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__ship_method, _hook__ship_method)
);

WITH staging AS (
  SELECT
    ship_method_id AS ship_method__ship_method_id,
    name AS ship_method__name,
    ship_base AS ship_method__ship_base,
    ship_rate AS ship_method__ship_rate,
    rowguid AS ship_method__rowguid,
    modified_date AS ship_method__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS ship_method__record_loaded_at
  FROM das.raw__adventure_works__ship_methods
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ship_method__ship_method_id ORDER BY ship_method__record_loaded_at) AS ship_method__record_version,
    CASE
      WHEN ship_method__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE ship_method__record_loaded_at
    END AS ship_method__record_valid_from,
    COALESCE(
      LEAD(ship_method__record_loaded_at) OVER (PARTITION BY ship_method__ship_method_id ORDER BY ship_method__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS ship_method__record_valid_to,
    ship_method__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS ship_method__is_current_record,
    CASE
      WHEN ship_method__is_current_record
      THEN ship_method__record_loaded_at
      ELSE ship_method__record_valid_to
    END AS ship_method__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'ship_method__adventure_works|',
      ship_method__ship_method_id,
      '~epoch|valid_from|',
      ship_method__record_valid_from
    )::BLOB AS _pit_hook__ship_method,
    CONCAT('ship_method__adventure_works|', ship_method__ship_method_id) AS _hook__ship_method,
    *
  FROM validity
)
SELECT
  _pit_hook__ship_method::BLOB,
  _hook__ship_method::BLOB,
  ship_method__ship_method_id::BIGINT,
  ship_method__name::TEXT,
  ship_method__ship_base::DOUBLE,
  ship_method__ship_rate::DOUBLE,
  ship_method__rowguid::TEXT,
  ship_method__modified_date::DATE,
  ship_method__record_loaded_at::TIMESTAMP,
  ship_method__record_updated_at::TIMESTAMP,
  ship_method__record_version::TEXT,
  ship_method__record_valid_from::TIMESTAMP,
  ship_method__record_valid_to::TIMESTAMP,
  ship_method__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND ship_method__record_updated_at BETWEEN @start_ts AND @end_ts
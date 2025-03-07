MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__sales_reason,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__sales_reason, _hook__reference__sales_reason)
);

WITH staging AS (
  SELECT
    sales_reason_id AS sales_reason__sales_reason_id,
    name AS sales_reason__name,
    reason_type AS sales_reason__reason_type,
    modified_date AS sales_reason__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_reason__record_loaded_at
  FROM bronze.raw__adventure_works__sales_reasons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_reason__sales_reason_id ORDER BY sales_reason__record_loaded_at) AS sales_reason__record_version,
    CASE
      WHEN sales_reason__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_reason__record_loaded_at
    END AS sales_reason__record_valid_from,
    COALESCE(
      LEAD(sales_reason__record_loaded_at) OVER (PARTITION BY sales_reason__sales_reason_id ORDER BY sales_reason__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_reason__record_valid_to,
    sales_reason__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_reason__is_current_record,
    CASE
      WHEN sales_reason__is_current_record
      THEN sales_reason__record_loaded_at
      ELSE sales_reason__record_valid_to
    END AS sales_reason__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__sales_reason__adventure_works|',
      sales_reason__sales_reason_id,
      '~epoch|valid_from|',
      sales_reason__record_valid_from
    )::BLOB AS _pit_hook__reference__sales_reason,
    CONCAT('reference__sales_reason__adventure_works|', sales_reason__sales_reason_id) AS _hook__reference__sales_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__sales_reason::BLOB,
  _hook__reference__sales_reason::BLOB,
  sales_reason__sales_reason_id::BIGINT,
  sales_reason__name::TEXT,
  sales_reason__reason_type::TEXT,
  sales_reason__modified_date::DATE,
  sales_reason__record_loaded_at::TIMESTAMP,
  sales_reason__record_updated_at::TIMESTAMP,
  sales_reason__record_version::TEXT,
  sales_reason__record_valid_from::TIMESTAMP,
  sales_reason__record_valid_to::TIMESTAMP,
  sales_reason__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND sales_reason__record_updated_at BETWEEN @start_ts AND @end_ts
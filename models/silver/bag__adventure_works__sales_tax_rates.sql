MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__sales_tax_rate,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__sales_tax_rate, _hook__reference__sales_tax_rate),
  references (_hook__reference__state_province)
);

WITH staging AS (
  SELECT
    sales_tax_rate_id AS sales_tax_rate__sales_tax_rate_id,
    state_province_id AS sales_tax_rate__state_province_id,
    tax_type AS sales_tax_rate__tax_type,
    tax_rate AS sales_tax_rate__tax_rate,
    name AS sales_tax_rate__name,
    rowguid AS sales_tax_rate__rowguid,
    modified_date AS sales_tax_rate__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_tax_rate__record_loaded_at
  FROM bronze.raw__adventure_works__sales_tax_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at) AS sales_tax_rate__record_version,
    CASE
      WHEN sales_tax_rate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_tax_rate__record_loaded_at
    END AS sales_tax_rate__record_valid_from,
    COALESCE(
      LEAD(sales_tax_rate__record_loaded_at) OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_tax_rate__record_valid_to,
    sales_tax_rate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_tax_rate__is_current_record,
    CASE
      WHEN sales_tax_rate__is_current_record
      THEN sales_tax_rate__record_loaded_at
      ELSE sales_tax_rate__record_valid_to
    END AS sales_tax_rate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__sales_tax_rate__adventure_works|',
      sales_tax_rate__sales_tax_rate_id,
      '~epoch|valid_from|',
      sales_tax_rate__record_valid_from
    )::BLOB AS _pit_hook__reference__sales_tax_rate,
    CONCAT('reference__sales_tax_rate__adventure_works|', sales_tax_rate__sales_tax_rate_id) AS _hook__reference__sales_tax_rate,
    CONCAT('reference__state_province__adventure_works|', sales_tax_rate__state_province_id) AS _hook__reference__state_province,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__sales_tax_rate::BLOB,
  _hook__reference__sales_tax_rate::BLOB,
  _hook__reference__state_province::BLOB,
  sales_tax_rate__sales_tax_rate_id::BIGINT,
  sales_tax_rate__state_province_id::BIGINT,
  sales_tax_rate__tax_type::BIGINT,
  sales_tax_rate__tax_rate::DOUBLE,
  sales_tax_rate__name::TEXT,
  sales_tax_rate__rowguid::UUID,
  sales_tax_rate__modified_date::DATE,
  sales_tax_rate__record_loaded_at::TIMESTAMP,
  sales_tax_rate__record_updated_at::TIMESTAMP,
  sales_tax_rate__record_version::TEXT,
  sales_tax_rate__record_valid_from::TIMESTAMP,
  sales_tax_rate__record_valid_to::TIMESTAMP,
  sales_tax_rate__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND sales_tax_rate__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__illustration,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__reference__illustration, _hook__reference__illustration),
  references (_hook__reference__product_model)
);

WITH staging AS (
  SELECT
    product_model_id AS product_model_illustration__product_model_id,
    illustration_id AS product_model_illustration__illustration_id,
    modified_date AS product_model_illustration__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_model_illustration__record_loaded_at
  FROM das.raw__adventure_works__product_model_illustrations
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_model_illustration__illustration_id ORDER BY product_model_illustration__record_loaded_at) AS product_model_illustration__record_version,
    CASE
      WHEN product_model_illustration__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_model_illustration__record_loaded_at
    END AS product_model_illustration__record_valid_from,
    COALESCE(
      LEAD(product_model_illustration__record_loaded_at) OVER (PARTITION BY product_model_illustration__illustration_id ORDER BY product_model_illustration__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_model_illustration__record_valid_to,
    product_model_illustration__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_model_illustration__is_current_record,
    CASE
      WHEN product_model_illustration__is_current_record
      THEN product_model_illustration__record_loaded_at
      ELSE product_model_illustration__record_valid_to
    END AS product_model_illustration__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__illustration__adventure_works|',
      product_model_illustration__illustration_id,
      '~epoch__valid_from|',
      product_model_illustration__record_valid_from
    )::BLOB AS _pit_hook__reference__illustration,
    CONCAT('reference__illustration__adventure_works|', product_model_illustration__illustration_id) AS _hook__reference__illustration,
    CONCAT('reference__product_model__adventure_works|', product_model_illustration__product_model_id) AS _hook__reference__product_model,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__illustration::BLOB,
  _hook__reference__illustration::BLOB,
  _hook__reference__product_model::BLOB,
  product_model_illustration__product_model_id::BIGINT,
  product_model_illustration__illustration_id::BIGINT,
  product_model_illustration__modified_date::DATE,
  product_model_illustration__record_loaded_at::TIMESTAMP,
  product_model_illustration__record_updated_at::TIMESTAMP,
  product_model_illustration__record_version::TEXT,
  product_model_illustration__record_valid_from::TIMESTAMP,
  product_model_illustration__record_valid_to::TIMESTAMP,
  product_model_illustration__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_model_illustration__record_updated_at BETWEEN @start_ts AND @end_ts
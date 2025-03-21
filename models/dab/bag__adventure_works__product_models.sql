MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__product_model
  ),
  tags hook,
  grain (_pit_hook__reference__product_model, _hook__reference__product_model),
  description 'Hook viewpoint of product_models data: Product model classification.',
  column_descriptions (
    product_model__product_model_id = 'Primary key for ProductModel records.',
    product_model__name = 'Product model description.',
    product_model__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product_model__catalog_description = 'Detailed product catalog information in xml format.',
    product_model__instructions = 'Manufacturing instructions in xml format.',
    product_model__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_model__record_updated_at = 'Timestamp when this record was last updated',
    product_model__record_version = 'Version number for this record',
    product_model__record_valid_from = 'Timestamp from which this record version is valid',
    product_model__record_valid_to = 'Timestamp until which this record version is valid',
    product_model__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__product_model = 'Reference hook to product_model reference',
    _pit_hook__reference__product_model = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    product_model_id AS product_model__product_model_id,
    name AS product_model__name,
    rowguid AS product_model__rowguid,
    catalog_description AS product_model__catalog_description,
    instructions AS product_model__instructions,
    modified_date AS product_model__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_model__record_loaded_at
  FROM das.raw__adventure_works__product_models
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_model__product_model_id ORDER BY product_model__record_loaded_at) AS product_model__record_version,
    CASE
      WHEN product_model__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_model__record_loaded_at
    END AS product_model__record_valid_from,
    COALESCE(
      LEAD(product_model__record_loaded_at) OVER (PARTITION BY product_model__product_model_id ORDER BY product_model__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_model__record_valid_to,
    product_model__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_model__is_current_record,
    CASE
      WHEN product_model__is_current_record
      THEN product_model__record_loaded_at
      ELSE product_model__record_valid_to
    END AS product_model__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__product_model__adventure_works|', product_model__product_model_id) AS _hook__reference__product_model,
    CONCAT_WS('~',
      _hook__reference__product_model,
      'epoch__valid_from|'||product_model__record_valid_from
    ) AS _pit_hook__reference__product_model,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__product_model::BLOB,
  _hook__reference__product_model::BLOB,
  product_model__product_model_id::BIGINT,
  product_model__name::TEXT,
  product_model__rowguid::TEXT,
  product_model__catalog_description::TEXT,
  product_model__instructions::TEXT,
  product_model__modified_date::DATE,
  product_model__record_loaded_at::TIMESTAMP,
  product_model__record_updated_at::TIMESTAMP,
  product_model__record_version::TEXT,
  product_model__record_valid_from::TIMESTAMP,
  product_model__record_valid_to::TIMESTAMP,
  product_model__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_model__record_updated_at BETWEEN @start_ts AND @end_ts
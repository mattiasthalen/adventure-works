MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product_list_price_history),
  description 'Business viewpoint of product_list_price_histories data: Changes in the list price of a product over time.',
  column_descriptions (
    product_list_price_history__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    product_list_price_history__start_date = 'List price start date.',
    product_list_price_history__end_date = 'List price end date.',
    product_list_price_history__list_price = 'Product list price.',
    product_list_price_history__modified_date = 'Date when this record was last modified',
    product_list_price_history__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product_list_price_history__record_updated_at = 'Timestamp when this record was last updated',
    product_list_price_history__record_version = 'Version number for this record',
    product_list_price_history__record_valid_from = 'Timestamp from which this record version is valid',
    product_list_price_history__record_valid_to = 'Timestamp until which this record version is valid',
    product_list_price_history__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product_list_price_history,
    product_list_price_history__product_id,
    product_list_price_history__start_date,
    product_list_price_history__end_date,
    product_list_price_history__list_price,
    product_list_price_history__modified_date,
    product_list_price_history__record_loaded_at,
    product_list_price_history__record_updated_at,
    product_list_price_history__record_version,
    product_list_price_history__record_valid_from,
    product_list_price_history__record_valid_to,
    product_list_price_history__is_current_record
  FROM dab.bag__adventure_works__product_list_price_histories
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product_list_price_history,
    NULL AS product_list_price_history__product_id,
    NULL AS product_list_price_history__start_date,
    NULL AS product_list_price_history__end_date,
    NULL AS product_list_price_history__list_price,
    NULL AS product_list_price_history__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product_list_price_history__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product_list_price_history__record_updated_at,
    0 AS product_list_price_history__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product_list_price_history__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product_list_price_history__record_valid_to,
    TRUE AS product_list_price_history__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product_list_price_history::BLOB,
  product_list_price_history__product_id::BIGINT,
  product_list_price_history__start_date::DATE,
  product_list_price_history__end_date::DATE,
  product_list_price_history__list_price::DOUBLE,
  product_list_price_history__modified_date::DATE,
  product_list_price_history__record_loaded_at::TIMESTAMP,
  product_list_price_history__record_updated_at::TIMESTAMP,
  product_list_price_history__record_version::TEXT,
  product_list_price_history__record_valid_from::TIMESTAMP,
  product_list_price_history__record_valid_to::TIMESTAMP,
  product_list_price_history__is_current_record::BOOLEAN
FROM cte__final
MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product_list_price_history
  ),
  tags hook,
  grain (_pit_hook__product_list_price_history, _hook__product_list_price_history),
  references (_hook__product, _hook__epoch__start_date)
);

WITH staging AS (
  SELECT
    product_id AS product_list_price_history__product_id,
    start_date AS product_list_price_history__start_date,
    end_date AS product_list_price_history__end_date,
    list_price AS product_list_price_history__list_price,
    modified_date AS product_list_price_history__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_list_price_history__record_loaded_at
  FROM das.raw__adventure_works__product_list_price_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_list_price_history__product_id, product_list_price_history__start_date ORDER BY product_list_price_history__record_loaded_at) AS product_list_price_history__record_version,
    CASE
      WHEN product_list_price_history__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product_list_price_history__record_loaded_at
    END AS product_list_price_history__record_valid_from,
    COALESCE(
      LEAD(product_list_price_history__record_loaded_at) OVER (PARTITION BY product_list_price_history__product_id, product_list_price_history__start_date ORDER BY product_list_price_history__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product_list_price_history__record_valid_to,
    product_list_price_history__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product_list_price_history__is_current_record,
    CASE
      WHEN product_list_price_history__is_current_record
      THEN product_list_price_history__record_loaded_at
      ELSE product_list_price_history__record_valid_to
    END AS product_list_price_history__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product__adventure_works|', product_list_price_history__product_id) AS _hook__product,
    CONCAT('epoch__date|', product_list_price_history__start_date) AS _hook__epoch__start_date,
    CONCAT_WS('~', _hook__product, _hook__epoch__start_date) AS _hook__product_list_price_history,
    CONCAT_WS('~',
      _hook__product_list_price_history,
      'epoch__valid_from|'||product_list_price_history__record_valid_from
    ) AS _pit_hook__product_list_price_history,
    *
  FROM validity
)
SELECT
  _pit_hook__product_list_price_history::BLOB,
  _hook__product_list_price_history::BLOB,
  _hook__product::BLOB,
  _hook__epoch__start_date::BLOB,
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
  product_list_price_history__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product_list_price_history__record_updated_at BETWEEN @start_ts AND @end_ts
MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column product_list_price_histori__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_id AS product_list_price_histori__product_id,
    end_date AS product_list_price_histori__end_date,
    list_price AS product_list_price_histori__list_price,
    modified_date AS product_list_price_histori__modified_date,
    start_date AS product_list_price_histori__start_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product_list_price_histori__record_loaded_at
  FROM bronze.raw__adventure_works__product_list_price_histories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product_list_price_histori__product_id ORDER BY product_list_price_histori__record_loaded_at) AS product_list_price_histori__record_version,
    CASE
      WHEN product_list_price_histori__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product_list_price_histori__record_loaded_at
    END AS product_list_price_histori__record_valid_from,
    COALESCE(
      LEAD(product_list_price_histori__record_loaded_at) OVER (PARTITION BY product_list_price_histori__product_id ORDER BY product_list_price_histori__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product_list_price_histori__record_valid_to,
    product_list_price_histori__record_valid_to = @max_ts::TIMESTAMP AS product_list_price_histori__is_current_record,
    CASE
      WHEN product_list_price_histori__is_current_record
      THEN product_list_price_histori__record_loaded_at
      ELSE product_list_price_histori__record_valid_to
    END AS product_list_price_histori__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product_list_price_histori__product_id,
      '~epoch|valid_from|',
      product_list_price_histori__record_valid_from
    ) AS _pit_hook__product,
    CONCAT('product|adventure_works|', product_list_price_histori__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  product_list_price_histori__product_id::BIGINT,
  product_list_price_histori__end_date::VARCHAR,
  product_list_price_histori__list_price::DOUBLE,
  product_list_price_histori__modified_date::VARCHAR,
  product_list_price_histori__start_date::TIMESTAMP,
  product_list_price_histori__record_loaded_at::TIMESTAMP,
  product_list_price_histori__record_updated_at::TIMESTAMP,
  product_list_price_histori__record_valid_from::TIMESTAMP,
  product_list_price_histori__record_valid_to::TIMESTAMP,
  product_list_price_histori__record_version::INT,
  product_list_price_histori__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND product_list_price_histori__record_updated_at BETWEEN @start_ts AND @end_ts
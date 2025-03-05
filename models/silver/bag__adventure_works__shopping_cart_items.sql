MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column shopping_cart_item__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__shopping_cart_item, _hook__shopping_cart_item),
  references (_hook__product)
);

WITH staging AS (
  SELECT
    shopping_cart_item_id AS shopping_cart_item__shopping_cart_item_id,
    shopping_cart_id AS shopping_cart_item__shopping_cart_id,
    quantity AS shopping_cart_item__quantity,
    product_id AS shopping_cart_item__product_id,
    date_created AS shopping_cart_item__date_created,
    modified_date AS shopping_cart_item__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shopping_cart_item__record_loaded_at
  FROM bronze.raw__adventure_works__shopping_cart_items
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shopping_cart_item__shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at) AS shopping_cart_item__record_version,
    CASE
      WHEN shopping_cart_item__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE shopping_cart_item__record_loaded_at
    END AS shopping_cart_item__record_valid_from,
    COALESCE(
      LEAD(shopping_cart_item__record_loaded_at) OVER (PARTITION BY shopping_cart_item__shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS shopping_cart_item__record_valid_to,
    shopping_cart_item__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS shopping_cart_item__is_current_record,
    CASE
      WHEN shopping_cart_item__is_current_record
      THEN shopping_cart_item__record_loaded_at
      ELSE shopping_cart_item__record_valid_to
    END AS shopping_cart_item__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__adventure_works|',
      shopping_cart_item__shopping_cart_item_id,
      '~epoch__valid_from|',
      shopping_cart_item__record_valid_from
    )::BLOB AS _pit_hook__shopping_cart_item,
    CONCAT('reference__adventure_works|', shopping_cart_item__shopping_cart_item_id) AS _hook__shopping_cart_item,
    CONCAT('product__adventure_works|', shopping_cart_item__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__shopping_cart_item::BLOB,
  _hook__shopping_cart_item::BLOB,
  _hook__product::BLOB,
  shopping_cart_item__shopping_cart_item_id::BIGINT,
  shopping_cart_item__shopping_cart_id::TEXT,
  shopping_cart_item__quantity::BIGINT,
  shopping_cart_item__product_id::BIGINT,
  shopping_cart_item__date_created::TEXT,
  shopping_cart_item__modified_date::DATE,
  shopping_cart_item__record_loaded_at::TIMESTAMP,
  shopping_cart_item__record_updated_at::TIMESTAMP,
  shopping_cart_item__record_version::TEXT,
  shopping_cart_item__record_valid_from::TIMESTAMP,
  shopping_cart_item__record_valid_to::TIMESTAMP,
  shopping_cart_item__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND shopping_cart_item__record_updated_at BETWEEN @start_ts AND @end_ts
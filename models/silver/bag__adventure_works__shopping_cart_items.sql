MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column shopping_cart_item__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    shopping_cart_item_id AS shopping_cart_item__shopping_cart_item_id,
    product_id AS shopping_cart_item__product_id,
    shopping_cart_id AS shopping_cart_item__shopping_cart_id,
    date_created AS shopping_cart_item__date_created,
    modified_date AS shopping_cart_item__modified_date,
    quantity AS shopping_cart_item__quantity,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS shopping_cart_item__record_loaded_at
  FROM bronze.raw__adventure_works__shopping_cart_items
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY shopping_cart_item__shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at) AS shopping_cart_item__record_version,
    CASE
      WHEN shopping_cart_item__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE shopping_cart_item__record_loaded_at
    END AS shopping_cart_item__record_valid_from,
    COALESCE(
      LEAD(shopping_cart_item__record_loaded_at) OVER (PARTITION BY shopping_cart_item__shopping_cart_item_id ORDER BY shopping_cart_item__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS shopping_cart_item__record_valid_to,
    shopping_cart_item__record_valid_to = @max_ts::TIMESTAMP AS shopping_cart_item__is_current_record,
    CASE
      WHEN shopping_cart_item__is_current_record
      THEN shopping_cart_item__record_loaded_at
      ELSE shopping_cart_item__record_valid_to
    END AS shopping_cart_item__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'shopping_cart_item|adventure_works|',
      shopping_cart_item__shopping_cart_item_id,
      '~epoch|valid_from|',
      shopping_cart_item__record_valid_from
    ) AS _pit_hook__shopping_cart_item,
    CONCAT('shopping_cart_item|adventure_works|', shopping_cart_item__shopping_cart_item_id) AS _hook__shopping_cart_item,
    CONCAT('shopping_cart|adventure_works|', shopping_cart_item__shopping_cart_id) AS _hook__shopping_cart,
    CONCAT('product|adventure_works|', shopping_cart_item__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__shopping_cart_item::BLOB,
  _hook__shopping_cart_item::BLOB,
  _hook__product::BLOB,
  _hook__shopping_cart::BLOB,
  shopping_cart_item__shopping_cart_item_id::BIGINT,
  shopping_cart_item__product_id::BIGINT,
  shopping_cart_item__shopping_cart_id::VARCHAR,
  shopping_cart_item__date_created::VARCHAR,
  shopping_cart_item__modified_date::VARCHAR,
  shopping_cart_item__quantity::BIGINT,
  shopping_cart_item__record_loaded_at::TIMESTAMP,
  shopping_cart_item__record_updated_at::TIMESTAMP,
  shopping_cart_item__record_valid_from::TIMESTAMP,
  shopping_cart_item__record_valid_to::TIMESTAMP,
  shopping_cart_item__record_version::INT,
  shopping_cart_item__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND shopping_cart_item__record_updated_at BETWEEN @start_ts AND @end_ts
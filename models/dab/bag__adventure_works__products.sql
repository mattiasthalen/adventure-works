MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product
  ),
  tags hook,
  grain (_pit_hook__product, _hook__product),
  references (_hook__product_subcategory, _hook__reference__product_model)
);

WITH staging AS (
  SELECT
    product_id AS product__product_id,
    name AS product__name,
    product_number AS product__product_number,
    make_flag AS product__make_flag,
    finished_goods_flag AS product__finished_goods_flag,
    safety_stock_level AS product__safety_stock_level,
    reorder_point AS product__reorder_point,
    standard_cost AS product__standard_cost,
    list_price AS product__list_price,
    days_to_manufacture AS product__days_to_manufacture,
    sell_start_date AS product__sell_start_date,
    rowguid AS product__rowguid,
    color AS product__color,
    class AS product__class,
    weight_unit_measure_code AS product__weight_unit_measure_code,
    weight AS product__weight,
    size AS product__size,
    size_unit_measure_code AS product__size_unit_measure_code,
    product_line AS product__product_line,
    style AS product__style,
    product_subcategory_id AS product__product_subcategory_id,
    product_model_id AS product__product_model_id,
    sell_end_date AS product__sell_end_date,
    modified_date AS product__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product__record_loaded_at
  FROM das.raw__adventure_works__products
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at) AS product__record_version,
    CASE
      WHEN product__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE product__record_loaded_at
    END AS product__record_valid_from,
    COALESCE(
      LEAD(product__record_loaded_at) OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS product__record_valid_to,
    product__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS product__is_current_record,
    CASE
      WHEN product__is_current_record
      THEN product__record_loaded_at
      ELSE product__record_valid_to
    END AS product__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('product__adventure_works|', product__product_id) AS _hook__product,
    CONCAT('product_subcategory__adventure_works|', product__product_subcategory_id) AS _hook__product_subcategory,
    CONCAT('reference__product_model__adventure_works|', product__product_model_id) AS _hook__reference__product_model,
    CONCAT_WS('~',
      _hook__product,
      'epoch__valid_from|'||product__record_valid_from
    ) AS _pit_hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  _hook__product_subcategory::BLOB,
  _hook__reference__product_model::BLOB,
  product__product_id::BIGINT,
  product__name::TEXT,
  product__product_number::TEXT,
  product__make_flag::BOOLEAN,
  product__finished_goods_flag::BOOLEAN,
  product__safety_stock_level::BIGINT,
  product__reorder_point::BIGINT,
  product__standard_cost::DOUBLE,
  product__list_price::DOUBLE,
  product__days_to_manufacture::BIGINT,
  product__sell_start_date::DATE,
  product__rowguid::TEXT,
  product__color::TEXT,
  product__class::TEXT,
  product__weight_unit_measure_code::TEXT,
  product__weight::DOUBLE,
  product__size::TEXT,
  product__size_unit_measure_code::TEXT,
  product__product_line::TEXT,
  product__style::TEXT,
  product__product_subcategory_id::BIGINT,
  product__product_model_id::BIGINT,
  product__sell_end_date::DATE,
  product__modified_date::DATE,
  product__record_loaded_at::TIMESTAMP,
  product__record_updated_at::TIMESTAMP,
  product__record_version::TEXT,
  product__record_valid_from::TIMESTAMP,
  product__record_valid_to::TIMESTAMP,
  product__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND product__record_updated_at BETWEEN @start_ts AND @end_ts
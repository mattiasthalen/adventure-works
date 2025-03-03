MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    product_id AS product__product_id,
    product_model_id AS product__product_model_id,
    product_subcategory_id AS product__product_subcategory_id,
    class AS product__class,
    color AS product__color,
    days_to_manufacture AS product__days_to_manufacture,
    finished_goods_flag AS product__finished_goods_flag,
    list_price AS product__list_price,
    make_flag AS product__make_flag,
    modified_date AS product__modified_date,
    name AS product__name,
    product_line AS product__product_line,
    product_number AS product__product_number,
    reorder_point AS product__reorder_point,
    rowguid AS product__rowguid,
    safety_stock_level AS product__safety_stock_level,
    sell_end_date AS product__sell_end_date,
    sell_start_date AS product__sell_start_date,
    size AS product__size,
    size_unit_measure_code AS product__size_unit_measure_code,
    standard_cost AS product__standard_cost,
    style AS product__style,
    weight AS product__weight,
    weight_unit_measure_code AS product__weight_unit_measure_code,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS product__record_loaded_at
  FROM bronze.raw__adventure_works__products
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at) AS product__record_version,
    CASE
      WHEN product__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE product__record_loaded_at
    END AS product__record_valid_from,
    COALESCE(
      LEAD(product__record_loaded_at) OVER (PARTITION BY product__product_id ORDER BY product__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS product__record_valid_to,
    product__record_valid_to = @max_ts::TIMESTAMP AS product__is_current_record,
    CASE
      WHEN product__is_current_record
      THEN product__record_loaded_at
      ELSE product__record_valid_to
    END AS product__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product|adventure_works|',
      product__product_id,
      '~epoch|valid_from|',
      product__record_valid_from
    ) AS _pit_hook__product,
    CONCAT('product|adventure_works|', product__product_id) AS _hook__product,
    CONCAT('product_model|adventure_works|', product__product_model_id) AS _hook__product_model,
    CONCAT('product_subcategory|adventure_works|', product__product_subcategory_id) AS _hook__product_subcategory,
    *
  FROM validity
)
SELECT
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  _hook__product_model::BLOB,
  _hook__product_subcategory::BLOB,
  product__product_id::VARCHAR,
  product__product_model_id::VARCHAR,
  product__product_subcategory_id::VARCHAR,
  product__class::VARCHAR,
  product__color::VARCHAR,
  product__days_to_manufacture::VARCHAR,
  product__finished_goods_flag::VARCHAR,
  product__list_price::VARCHAR,
  product__make_flag::VARCHAR,
  product__modified_date::VARCHAR,
  product__name::VARCHAR,
  product__product_line::VARCHAR,
  product__product_number::VARCHAR,
  product__reorder_point::VARCHAR,
  product__rowguid::VARCHAR,
  product__safety_stock_level::VARCHAR,
  product__sell_end_date::VARCHAR,
  product__sell_start_date::VARCHAR,
  product__size::VARCHAR,
  product__size_unit_measure_code::VARCHAR,
  product__standard_cost::VARCHAR,
  product__style::VARCHAR,
  product__weight::VARCHAR,
  product__weight_unit_measure_code::VARCHAR,
  product__record_loaded_at::TIMESTAMP,
  product__record_version::INT,
  product__record_valid_from::TIMESTAMP,
  product__record_valid_to::TIMESTAMP,
  product__is_current_record::BOOLEAN,
  product__record_updated_at::TIMESTAMP
FROM hooks
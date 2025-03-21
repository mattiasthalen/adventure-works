MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__product),
  description 'Business viewpoint of products data: Products sold or used in the manufacturing of sold products.',
  column_descriptions (
    product__product_id = 'Primary key for Product records.',
    product__name = 'Name of the product.',
    product__product_number = 'Unique product identification number.',
    product__make_flag = '0 = Product is purchased, 1 = Product is manufactured in-house.',
    product__finished_goods_flag = '0 = Product is not a salable item. 1 = Product is salable.',
    product__safety_stock_level = 'Minimum inventory quantity.',
    product__reorder_point = 'Inventory level that triggers a purchase order or work order.',
    product__standard_cost = 'Standard cost of the product.',
    product__list_price = 'Selling price.',
    product__days_to_manufacture = 'Number of days required to manufacture the product.',
    product__sell_start_date = 'Date the product was available for sale.',
    product__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    product__color = 'Product color.',
    product__class = 'H = High, M = Medium, L = Low.',
    product__weight_unit_measure_code = 'Unit of measure for Weight column.',
    product__weight = 'Product weight.',
    product__size = 'Product size.',
    product__size_unit_measure_code = 'Unit of measure for Size column.',
    product__product_line = 'R = Road, M = Mountain, T = Touring, S = Standard.',
    product__style = 'W = Womens, M = Mens, U = Universal.',
    product__product_subcategory_id = 'Product is a member of this product subcategory. Foreign key to ProductSubCategory.ProductSubCategoryID.',
    product__product_model_id = 'Product is a member of this product model. Foreign key to ProductModel.ProductModelID.',
    product__sell_end_date = 'Date the product was no longer available for sale.',
    product__modified_date = 'Date when this record was last modified',
    product__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product__record_updated_at = 'Timestamp when this record was last updated',
    product__record_version = 'Version number for this record',
    product__record_valid_from = 'Timestamp from which this record version is valid',
    product__record_valid_to = 'Timestamp until which this record version is valid',
    product__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__product,
    product__product_id,
    product__name,
    product__product_number,
    product__make_flag,
    product__finished_goods_flag,
    product__safety_stock_level,
    product__reorder_point,
    product__standard_cost,
    product__list_price,
    product__days_to_manufacture,
    product__sell_start_date,
    product__rowguid,
    product__color,
    product__class,
    product__weight_unit_measure_code,
    product__weight,
    product__size,
    product__size_unit_measure_code,
    product__product_line,
    product__style,
    product__product_subcategory_id,
    product__product_model_id,
    product__sell_end_date,
    product__modified_date,
    product__record_loaded_at,
    product__record_updated_at,
    product__record_version,
    product__record_valid_from,
    product__record_valid_to,
    product__is_current_record
  FROM dab.bag__adventure_works__products
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__product,
    NULL AS product__product_id,
    'N/A' AS product__name,
    'N/A' AS product__product_number,
    FALSE AS product__make_flag,
    FALSE AS product__finished_goods_flag,
    NULL AS product__safety_stock_level,
    NULL AS product__reorder_point,
    NULL AS product__standard_cost,
    NULL AS product__list_price,
    NULL AS product__days_to_manufacture,
    NULL AS product__sell_start_date,
    'N/A' AS product__rowguid,
    'N/A' AS product__color,
    'N/A' AS product__class,
    'N/A' AS product__weight_unit_measure_code,
    NULL AS product__weight,
    'N/A' AS product__size,
    'N/A' AS product__size_unit_measure_code,
    'N/A' AS product__product_line,
    'N/A' AS product__style,
    NULL AS product__product_subcategory_id,
    NULL AS product__product_model_id,
    NULL AS product__sell_end_date,
    NULL AS product__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS product__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS product__record_updated_at,
    0 AS product__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS product__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS product__record_valid_to,
    TRUE AS product__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__product::BLOB,
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
  product__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.products TO './export/dar/products.parquet' (FORMAT parquet, COMPRESSION zstd)
);
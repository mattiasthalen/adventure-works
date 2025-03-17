MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__product
  ),
  tags hook,
  grain (_pit_hook__product, _hook__product),
  description 'Hook viewpoint of products data: Products sold or used in the manufacturing of sold products.',
  references (_hook__product_subcategory, _hook__reference__product_model),
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
    product__record_loaded_at = 'Timestamp when this record was loaded into the system',
    product__record_updated_at = 'Timestamp when this record was last updated',
    product__record_version = 'Version number for this record',
    product__record_valid_from = 'Timestamp from which this record version is valid',
    product__record_valid_to = 'Timestamp until which this record version is valid',
    product__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__product = 'Reference hook to product',
    _hook__product_subcategory = 'Reference hook to product_subcategory',
    _hook__reference__product_model = 'Reference hook to product_model reference',
    _pit_hook__product = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
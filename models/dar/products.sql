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

SELECT
  *
  EXCLUDE (_hook__product, _hook__product_subcategory, _hook__reference__product_model)
FROM dab.bag__adventure_works__products
MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of products data: Products sold or used in the manufacturing of sold products.',
  column_descriptions (
    product_id = 'Primary key for Product records.',
    name = 'Name of the product.',
    product_number = 'Unique product identification number.',
    make_flag = '0 = Product is purchased, 1 = Product is manufactured in-house.',
    finished_goods_flag = '0 = Product is not a salable item. 1 = Product is salable.',
    safety_stock_level = 'Minimum inventory quantity.',
    reorder_point = 'Inventory level that triggers a purchase order or work order.',
    standard_cost = 'Standard cost of the product.',
    list_price = 'Selling price.',
    days_to_manufacture = 'Number of days required to manufacture the product.',
    sell_start_date = 'Date the product was available for sale.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    color = 'Product color.',
    class = 'H = High, M = Medium, L = Low.',
    weight_unit_measure_code = 'Unit of measure for Weight column.',
    weight = 'Product weight.',
    size = 'Product size.',
    size_unit_measure_code = 'Unit of measure for Size column.',
    product_line = 'R = Road, M = Mountain, T = Touring, S = Standard.',
    style = 'W = Womens, M = Mens, U = Universal.',
    product_subcategory_id = 'Product is a member of this product subcategory. Foreign key to ProductSubCategory.ProductSubCategoryID.',
    product_model_id = 'Product is a member of this product model. Foreign key to ProductModel.ProductModelID.',
    sell_end_date = 'Date the product was no longer available for sale.'
  )
);

SELECT
    product_id::BIGINT,
    name::TEXT,
    product_number::TEXT,
    make_flag::BOOLEAN,
    finished_goods_flag::BOOLEAN,
    safety_stock_level::BIGINT,
    reorder_point::BIGINT,
    standard_cost::DOUBLE,
    list_price::DOUBLE,
    days_to_manufacture::BIGINT,
    sell_start_date::DATE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    color::TEXT,
    class::TEXT,
    weight_unit_measure_code::TEXT,
    weight::DOUBLE,
    size::TEXT,
    size_unit_measure_code::TEXT,
    product_line::TEXT,
    style::TEXT,
    product_subcategory_id::BIGINT,
    product_model_id::BIGINT,
    sell_end_date::DATE
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__products"
)
;
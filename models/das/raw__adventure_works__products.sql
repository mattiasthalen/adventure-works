MODEL (
  kind VIEW,
  enabled TRUE
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
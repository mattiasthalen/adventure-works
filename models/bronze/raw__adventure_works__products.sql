MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_id::BIGINT,
  product_model_id::BIGINT,
  product_subcategory_id::BIGINT,
  class::VARCHAR,
  color::VARCHAR,
  days_to_manufacture::BIGINT,
  finished_goods_flag::BOOLEAN,
  list_price::DOUBLE,
  make_flag::BOOLEAN,
  modified_date::VARCHAR,
  name::VARCHAR,
  product_line::VARCHAR,
  product_number::VARCHAR,
  reorder_point::BIGINT,
  rowguid::VARCHAR,
  safety_stock_level::BIGINT,
  sell_end_date::VARCHAR,
  sell_start_date::VARCHAR,
  size::VARCHAR,
  size_unit_measure_code::VARCHAR,
  standard_cost::DOUBLE,
  style::VARCHAR,
  weight::DOUBLE,
  weight_unit_measure_code::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__products"
);

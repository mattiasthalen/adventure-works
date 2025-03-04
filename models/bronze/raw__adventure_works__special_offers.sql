MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  special_offer_id::BIGINT,
  category::VARCHAR,
  description::VARCHAR,
  discount_percentage::DOUBLE,
  end_date::VARCHAR,
  maximum_quantity::BIGINT,
  minimum_quantity::BIGINT,
  modified_date::VARCHAR,
  rowguid::VARCHAR,
  start_date::VARCHAR,
  type::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__special_offers"
);

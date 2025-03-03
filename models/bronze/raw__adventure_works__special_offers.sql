MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  special_offer_id,
  category,
  description,
  discount_percentage,
  end_date,
  maximum_quantity,
  minimum_quantity,
  modified_date,
  rowguid,
  start_date,
  type,
  _dlt_load_id
  FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__special_offers"
  )
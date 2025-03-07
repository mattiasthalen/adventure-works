MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    special_offer_id::BIGINT,
    description::TEXT,
    discount_percentage::DOUBLE,
    type::TEXT,
    category::TEXT,
    start_date::TEXT,
    end_date::TEXT,
    minimum_quantity::BIGINT,
    rowguid::UUID,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    maximum_quantity::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__special_offers"
)
;
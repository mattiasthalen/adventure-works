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
    start_date::DATE,
    end_date::DATE,
    minimum_quantity::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    maximum_quantity::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__special_offers"
)
;
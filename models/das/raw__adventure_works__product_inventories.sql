MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_id::BIGINT,
    location_id::BIGINT,
    shelf::TEXT,
    bin::BIGINT,
    quantity::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_inventories"
)
;
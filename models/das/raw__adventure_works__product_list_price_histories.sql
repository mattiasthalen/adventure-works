MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_id::BIGINT,
    start_date::DATE,
    end_date::DATE,
    list_price::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_list_price_histories"
)
;
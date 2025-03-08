MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    transaction_id::BIGINT,
    product_id::BIGINT,
    reference_order_id::BIGINT,
    reference_order_line_id::BIGINT,
    transaction_date::DATE,
    transaction_type::TEXT,
    quantity::BIGINT,
    actual_cost::DOUBLE,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__transaction_history_archives"
)
;
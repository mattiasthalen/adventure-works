MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    work_order_id::BIGINT,
    product_id::BIGINT,
    order_qty::BIGINT,
    stocked_qty::BIGINT,
    scrapped_qty::BIGINT,
    start_date::DATE,
    end_date::DATE,
    due_date::DATE,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    scrap_reason_id::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__work_orders"
)
;
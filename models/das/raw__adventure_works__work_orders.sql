MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of work_orders data: Manufacturing work orders.',
  column_descriptions (
    work_order_id = 'Primary key for WorkOrder records.',
    product_id = 'Product identification number. Foreign key to Product.ProductID.',
    order_qty = 'Product quantity to build.',
    stocked_qty = 'Quantity built and put in inventory.',
    scrapped_qty = 'Quantity that failed inspection.',
    start_date = 'Work order start date.',
    end_date = 'Work order end date.',
    due_date = 'Work order due date.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    scrap_reason_id = 'Reason for inspection failure.'
  )
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
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__work_orders"
)
;
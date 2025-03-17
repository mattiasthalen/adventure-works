MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__work),
  description 'Business viewpoint of work_orders data: Manufacturing work orders.',
  column_descriptions (
    work_order__work_order_id = 'Primary key for WorkOrder records.',
    work_order__product_id = 'Product identification number. Foreign key to Product.ProductID.',
    work_order__order_qty = 'Product quantity to build.',
    work_order__stocked_qty = 'Quantity built and put in inventory.',
    work_order__scrapped_qty = 'Quantity that failed inspection.',
    work_order__start_date = 'Work order start date.',
    work_order__end_date = 'Work order end date.',
    work_order__due_date = 'Work order due date.',
    work_order__scrap_reason_id = 'Reason for inspection failure.',
    work_order__modified_date = 'Date when this record was last modified',
    work_order__record_loaded_at = 'Timestamp when this record was loaded into the system',
    work_order__record_updated_at = 'Timestamp when this record was last updated',
    work_order__record_version = 'Version number for this record',
    work_order__record_valid_from = 'Timestamp from which this record version is valid',
    work_order__record_valid_to = 'Timestamp until which this record version is valid',
    work_order__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__order__work, _hook__product, _hook__reference__scrap_reason)
FROM dab.bag__adventure_works__work_orders
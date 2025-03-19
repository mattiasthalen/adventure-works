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

WITH cte__source AS (
  SELECT
    _pit_hook__order__work,
    work_order__work_order_id,
    work_order__product_id,
    work_order__order_qty,
    work_order__stocked_qty,
    work_order__scrapped_qty,
    work_order__start_date,
    work_order__end_date,
    work_order__due_date,
    work_order__scrap_reason_id,
    work_order__modified_date,
    work_order__record_loaded_at,
    work_order__record_updated_at,
    work_order__record_version,
    work_order__record_valid_from,
    work_order__record_valid_to,
    work_order__is_current_record
  FROM dab.bag__adventure_works__work_orders
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__order__work,
    NULL AS work_order__work_order_id,
    NULL AS work_order__product_id,
    NULL AS work_order__order_qty,
    NULL AS work_order__stocked_qty,
    NULL AS work_order__scrapped_qty,
    NULL AS work_order__start_date,
    NULL AS work_order__end_date,
    NULL AS work_order__due_date,
    NULL AS work_order__scrap_reason_id,
    NULL AS work_order__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order__record_updated_at,
    0 AS work_order__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS work_order__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS work_order__record_valid_to,
    TRUE AS work_order__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__order__work::BLOB,
  work_order__work_order_id::BIGINT,
  work_order__product_id::BIGINT,
  work_order__order_qty::BIGINT,
  work_order__stocked_qty::BIGINT,
  work_order__scrapped_qty::BIGINT,
  work_order__start_date::DATE,
  work_order__end_date::DATE,
  work_order__due_date::DATE,
  work_order__scrap_reason_id::BIGINT,
  work_order__modified_date::DATE,
  work_order__record_loaded_at::TIMESTAMP,
  work_order__record_updated_at::TIMESTAMP,
  work_order__record_version::TEXT,
  work_order__record_valid_from::TIMESTAMP,
  work_order__record_valid_to::TIMESTAMP,
  work_order__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.work_orders TO './export/dar/work_orders.parquet' (FORMAT parquet, COMPRESSION zstd)
);